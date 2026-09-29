import nextCoreWebVitals from 'eslint-config-next/core-web-vitals';
import nextTypescript from 'eslint-config-next/typescript';
import tseslint from 'typescript-eslint';

const config = [
  {
    ignores: [
      'node_modules/**',
      '.next/**',
      'legacy/**',
      'media/**',
      'public/**',
      'drizzle/**',
      'coverage/**',
      'playwright-report/**',
      'test-results/**',
      'next-env.d.ts',
      'src/components/admin/ui/**',
    ],
  },
  ...nextCoreWebVitals,
  ...nextTypescript,
  {
    files: ['**/*.ts', '**/*.tsx'],
    plugins: { '@typescript-eslint': tseslint.plugin },
    rules: {
      '@typescript-eslint/no-explicit-any': 'error',
      '@typescript-eslint/ban-ts-comment': ['error', { 'ts-ignore': true, 'ts-nocheck': true, 'ts-expect-error': 'allow-with-description' }],
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_', varsIgnorePattern: '^_' }],
      '@typescript-eslint/consistent-type-imports': ['error', { fixStyle: 'inline-type-imports' }],
      'no-console': ['error', { allow: ['warn', 'error', 'info'] }],
      'no-warning-comments': ['error', { terms: ['todo', 'fixme', 'xxx', 'hack'], location: 'anywhere' }],
      'react/no-unescaped-entities': 'off',
    },
  },
  {
    files: ['scripts/**/*.{ts,mjs,js}', 'tests/**/*.{ts,tsx}'],
    rules: {
      'no-console': 'off',
    },
  },
];

export default config;
