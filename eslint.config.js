// @ts-check
import base from '@counsy-ai/config/eslint.base.js';
import tseslint from 'typescript-eslint';

export default tseslint.config(...base, {
  languageOptions: {
    parserOptions: {
      projectService: true,
      tsconfigRootDir: import.meta.dirname,
    },
  },
});
