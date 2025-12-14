import sharp from "sharp";

// Subtitle removal configuration
export const SUBTITLE_CROP_MARGIN_PX = 15; // Pixels to crop above detected subtitle position (kept for compatibility)

export type DetectOpts = {
  analysisWidth?: number;      // ancho de análisis (downscale)
  bottomFraction?: number;     // fracción inferior a analizar
  xPaddingFraction?: number;   // ignora bordes laterales
  smoothWindow?: number;       // suavizado 1D
  gapTolerance?: number;       // gaps permitidos
  highThr?: number;            // umbral fuerte
  lowThr?: number;             // umbral débil
  maxBandFraction?: number;    // seguridad: banda max relativa
  minBandPx?: number;          // seguridad: banda min
};

export type SubtitleBand = {
  startY: number;
  endY: number;
  confidence: number; // 0..~0.05 típicamente
};

export type CutDetectOpts = {
  analysisWidth?: number;        // default 720
  bottomFraction?: number;       // default 0.45 (más seguro)
  xPaddingFraction?: number;     // default 0.06
  xStep?: number;                // default 2 (performance)
  smoothWindow?: number;         // default 7
  gapTolerance?: number;         // default 4
  sigmaMultHigh?: number;        // default 2.4
  sigmaMultLow?: number;         // default 1.4
  minBandPx?: number;            // default 14 (en coords escaladas)
  maxCutFraction?: number;       // default 0.40 (no cortar más del 40%)
};

const DEFAULT_DETECT: Required<DetectOpts> = {
  analysisWidth: 720,
  bottomFraction: 0.35,
  xPaddingFraction: 0.06,
  smoothWindow: 5,
  gapTolerance: 3,
  highThr: 0.018,
  lowThr: 0.010,
  maxBandFraction: 0.22,
  minBandPx: 10
};

const DEFAULT_CUT_OPTS: Required<CutDetectOpts> = {
  analysisWidth: 720,
  bottomFraction: 0.45,
  xPaddingFraction: 0.06,
  xStep: 2,
  smoothWindow: 7,
  gapTolerance: 4,
  sigmaMultHigh: 2.4,
  sigmaMultLow: 1.4,
  minBandPx: 14,
  maxCutFraction: 0.40,
};


/**
 * Helper function: clamp value between min and max
 */
function clamp(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

/**
 * Helper function: smooth 1D array using moving average
 */
function smooth1D(arr: Float32Array, win: number): Float32Array {
  const n = arr.length;
  const w = Math.max(1, win | 0);
  if (w === 1) return arr;

  const half = Math.floor(w / 2);
  const out = new Float32Array(n);

  let sum = 0;
  let count = 0;

  for (let i = 0; i < Math.min(n, half + 1); i++) {
    sum += arr[i];
    count++;
  }

  for (let i = 0; i < n; i++) {
    const addIdx = i + half;
    const subIdx = i - half - 1;

    if (addIdx < n && addIdx !== half) {
      sum += arr[addIdx];
      count++;
    }
    if (subIdx >= 0) {
      sum -= arr[subIdx];
      count--;
    }

    out[i] = sum / Math.max(1, count);
  }

  return out;
}

/**
 * Detecta la banda de subtítulos (en coords del frame original).
 * Ultra rápido: downscale + densidad por fila + histeresis.
 */
export async function detectSubtitleBandFast(
  input: Buffer,
  opts: DetectOpts = {}
): Promise<SubtitleBand | null> {
  const o = { ...DEFAULT_DETECT, ...opts };

  const meta = await sharp(input).metadata();
  const W0 = meta.width ?? 0;
  const H0 = meta.height ?? 0;
  if (!W0 || !H0) return null;

  // Downscale
  const W = Math.min(o.analysisWidth, W0);
  const scale = W / W0;
  const H = Math.max(1, Math.round(H0 * scale));

  const gray = await sharp(input)
    .resize({ width: W })
    .greyscale()
    .raw()
    .toBuffer();

  const roiTop = Math.max(0, Math.floor(H * (1 - o.bottomFraction)));
  const roiH = H - roiTop;
  if (roiH < 20) return null;

  const xStart = Math.floor(W * o.xPaddingFraction);
  const xEnd = Math.ceil(W * (1 - o.xPaddingFraction));
  const rowW = Math.max(1, xEnd - xStart);

  // mean/std en ROI (para thresholds adaptativos)
  let sum = 0;
  let sum2 = 0;
  for (let y = roiTop; y < H; y++) {
    const base = y * W;
    for (let x = xStart; x < xEnd; x++) {
      const p = gray[base + x];
      sum += p;
      sum2 += p * p;
    }
  }
  const n = rowW * roiH;
  const mean = sum / n;
  const varr = Math.max(0, sum2 / n - mean * mean);
  const std = Math.sqrt(varr);

  // Umbrales adaptativos (bastante estables para subt blanco/outline)
  const brightThr = clamp(mean + 1.25 * std, 165, 240);
  const edgeStrongThr = clamp(0.90 * std + 18, 25, 90);
  const edgeBrightThr = clamp(0.55 * std + 10, 16, 70);

  const density = new Float32Array(roiH);

  for (let ry = 0; ry < roiH; ry++) {
    const y = roiTop + ry;
    const base = y * W;

    let hits = 0;

    for (let x = Math.max(xStart + 1, 1); x < Math.min(xEnd - 1, W - 1); x++) {
      const idx = base + x;
      const p = gray[idx];

      const gH = Math.abs(p - gray[idx - 1]) + Math.abs(p - gray[idx + 1]);
      const gV = y > 0 ? Math.abs(p - gray[idx - W]) : 0;
      const edge = Math.max(gH, gV);

      const isCandidate =
        (p >= brightThr && edge >= edgeBrightThr) ||
        (edge >= edgeStrongThr);

      if (isCandidate) hits++;
    }

    density[ry] = hits / rowW;
  }

  const smoothed = smooth1D(density, o.smoothWindow);

  // Buscar desde abajo (histeresis)
  let endRy = -1;
  for (let ry = roiH - 1; ry >= 0; ry--) {
    if (smoothed[ry] >= o.highThr) {
      endRy = ry;
      break;
    }
  }
  if (endRy === -1) return null;

  let startRy = endRy;
  let gaps = 0;
  for (let ry = endRy; ry >= 0; ry--) {
    if (smoothed[ry] >= o.lowThr) {
      startRy = ry;
      gaps = 0;
    } else {
      gaps++;
      if (gaps > o.gapTolerance) break;
    }
  }

  // Refinar extremos
  while (startRy < endRy && smoothed[startRy] < o.lowThr) startRy++;
  while (endRy > startRy && smoothed[endRy] < o.lowThr) endRy--;

  const bandHScaled = endRy - startRy + 1;
  if (bandHScaled < o.minBandPx) return null;

  // Convertir a coords originales
  const startYScaled = roiTop + startRy;
  const endYScaled = roiTop + endRy;

  const startY = Math.max(0, Math.floor(startYScaled / scale));
  const endY = Math.min(H0 - 1, Math.ceil(endYScaled / scale));
  const bandH = endY - startY + 1;

  // Guardrails para evitar falsos positivos que te comen media imagen
  if (startY < H0 * 0.45) return null;
  if (bandH / H0 > o.maxBandFraction) return null;

  let confSum = 0;
  for (let ry = startRy; ry <= endRy; ry++) confSum += smoothed[ry];
  const confidence = confSum / Math.max(1, bandHScaled);

  return { startY, endY, confidence };
}

/**
 * Detects subtitle region using fast pixel-based analysis.
 * Wrapper for detectSubtitleBandFast for backward compatibility.
 * 
 * @returns Object with startY and endY coordinates, or null if no subtitles detected
 */
export const detectSubtitleRegion = async (
  normalizedBuffer: Buffer
): Promise<{ startY: number; endY: number } | null> => {
  const band = await detectSubtitleBandFast(normalizedBuffer);
  if (!band) return null;
  return { startY: band.startY, endY: band.endY };
};

/**
 * Devuelve cutY en coords originales: cortarás todo lo que esté por debajo.
 * Si no detecta, devuelve null.
 */
export async function detectSubtitleCutY(
  input: Buffer,
  opts: CutDetectOpts = {}
): Promise<{ cutY: number; bandTopY: number; bandBottomY: number; score: number } | null> {
  const o = { ...DEFAULT_CUT_OPTS, ...opts };

  const meta = await sharp(input).metadata();
  const W0 = meta.width ?? 0;
  const H0 = meta.height ?? 0;
  if (!W0 || !H0) return null;

  // Downscale
  const W = Math.min(o.analysisWidth, W0);
  const scale = W / W0;
  const H = Math.max(1, Math.round(H0 * scale));

  const grayBuf = await sharp(input)
    .resize({ width: W })
    .greyscale()
    .raw()
    .toBuffer();

  const gray = new Uint8Array(grayBuf);

  // ROI bottom
  const roiTop = Math.max(0, Math.floor(H * (1 - o.bottomFraction)));
  const roiH = H - roiTop;
  if (roiH < 30) return null;

  const xStart = Math.floor(W * o.xPaddingFraction);
  const xEnd = Math.ceil(W * (1 - o.xPaddingFraction));

  const score = new Float32Array(roiH);

  // Score por fila: edges + bi-contrast (ideal para outline negro + relleno claro)
  for (let ry = 0; ry < roiH; ry++) {
    const y = roiTop + ry;
    const base = y * W;

    let edgeSum = 0;
    let bright = 0;
    let dark = 0;
    let samples = 0;

    for (let x = Math.max(xStart + 1, 1); x < Math.min(xEnd - 1, W - 1); x += o.xStep) {
      const idx = base + x;
      const p = gray[idx];

      // contraste típico de subt blanco con borde negro
      if (p > 220) bright++;
      if (p < 55) dark++;

      // edge energy rápido
      const gH = Math.abs(p - gray[idx - 1]) + Math.abs(p - gray[idx + 1]);
      const gV = y > 0 ? Math.abs(p - gray[idx - W]) : 0;
      edgeSum += Math.max(gH, gV);

      samples++;
    }

    const brightRatio = bright / Math.max(1, samples);
    const darkRatio = dark / Math.max(1, samples);
    const biContrast = Math.min(brightRatio, darkRatio); // outline+relleno => alto

    const edgeMean = edgeSum / Math.max(1, samples); // ~0..255
    const edgeNorm = edgeMean / 255.0;

    // pesos: edges + (biContrast * fuerte)
    score[ry] = (edgeNorm * 0.9) + (biContrast * 3.2);
  }

  const sm = smooth1D(score, o.smoothWindow);

  // threshold adaptativo sobre ROI
  let m = 0;
  for (let i = 0; i < roiH; i++) m += sm[i];
  m /= roiH;

  let v = 0;
  for (let i = 0; i < roiH; i++) {
    const d = sm[i] - m;
    v += d * d;
  }
  const std = Math.sqrt(v / roiH);

  const highThr = m + o.sigmaMultHigh * std;
  const lowThr = m + o.sigmaMultLow * std;

  // buscar desde abajo (IMPORTANT: siempre bottom-up)
  let end = -1;
  for (let i = roiH - 1; i >= 0; i--) {
    if (sm[i] >= highThr) {
      end = i;
      break;
    }
  }
  if (end === -1) return null;

  // expandir hacia arriba con histeresis
  let start = end;
  let gaps = 0;
  for (let i = end; i >= 0; i--) {
    if (sm[i] >= lowThr) {
      start = i;
      gaps = 0;
    } else {
      gaps++;
      if (gaps > o.gapTolerance) break;
    }
  }

  const bandH = end - start + 1;
  if (bandH < o.minBandPx) return null;

  // band coords escaladas => originales
  const bandTopScaled = roiTop + start;
  const bandBottomScaled = roiTop + end;

  const bandTopY = Math.max(0, Math.floor(bandTopScaled / scale));
  const bandBottomY = Math.min(H0 - 1, Math.ceil(bandBottomScaled / scale));

  // CUT: cortar un poco arriba del bandTopY
  const cutY = bandTopY;

  // guardrail: no cortar demasiado alto
  if (cutY < H0 * (1 - o.maxCutFraction)) {
    // estaría cortando más del maxCutFraction del frame => falso positivo
    return null;
  }

  // score promedio en banda
  let ssum = 0;
  for (let i = start; i <= end; i++) ssum += sm[i];
  const avgScore = ssum / bandH;

  return { cutY, bandTopY, bandBottomY, score: avgScore };
}

/**
 * Corta abajo (SUBS OUT). Si no detecta, hace fallback recortando un % fijo.
 * Siempre recorta la parte inferior donde están los subtítulos.
 */
export const removeSubtitlesFromBuffer = async (
  input: Buffer,
  {
    marginPx = 10,              // recortar un poquito arriba del texto
    fallbackRemoveFraction = 0.14, // si falla: cortar 14% del bottom
    detectOpts = {},
  }: {
    marginPx?: number;
    fallbackRemoveFraction?: number;
    detectOpts?: CutDetectOpts;
  } = {}
): Promise<Buffer> => {
  try {
    const img = sharp(input);
    const meta = await img.metadata();
    const W = meta.width ?? 0;
    const H = meta.height ?? 0;
    if (!W || !H) return img.png().toBuffer();

    const res = await detectSubtitleCutY(input, detectOpts);

    let cropH: number;
    if (res) {
      cropH = Math.max(1, res.cutY - marginPx);
      // safety
      cropH = clamp(cropH, 1, H);
      console.log(
        `removeSubtitlesFromBuffer: Subtitles detected, cutting at Y=${cropH} (band: ${res.bandTopY}-${res.bandBottomY}) in image ${W}x${H}`
      );
    } else {
      // fallback: SIEMPRE cortar bottom (requerimiento)
      cropH = Math.max(1, Math.floor(H * (1 - fallbackRemoveFraction)));
      console.log(
        `removeSubtitlesFromBuffer: No subtitles detected, using fallback: cutting ${fallbackRemoveFraction * 100}% from bottom (Y=${cropH}) in image ${W}x${H}`
      );
    }

    return img.extract({ left: 0, top: 0, width: W, height: cropH }).png().toBuffer();
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error("removeSubtitlesFromBuffer: Error during subtitle removal:", errorMessage);
    console.error("removeSubtitlesFromBuffer: Error stack:", error instanceof Error ? error.stack : "No stack");
    
    // Fallback to original image if processing fails
    console.log("removeSubtitlesFromBuffer: Returning original image as fallback");
    return sharp(input).png().toBuffer();
  }
};
