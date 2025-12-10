import { Document, Packer, Paragraph } from "docx";
import * as fs from "node:fs/promises";
import * as path from "node:path";

export async function writeDocxFromParagraphs(
  paragraphs: string[],
  outPath: string
): Promise<void> {
  const doc = new Document({
    sections: [
      {
        properties: {},
        children: paragraphs.flatMap((p, index) => {
          const nodes = [new Paragraph({ text: p })];
          if (index < paragraphs.length - 1) {
            nodes.push(new Paragraph({ text: "" }));
          }
          return nodes;
        }),
      },
    ],
  });

  const buffer = await Packer.toBuffer(doc);
  await fs.mkdir(path.dirname(outPath), { recursive: true });
  await fs.writeFile(outPath, buffer);
}