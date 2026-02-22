# Sufferpedia Explorer (Frontend)

Next.js 16 frontend for browsing parsed testimony data from `../public_data/processed`.

## Requirements
- Node.js `>=20.9.0`
- npm `>=10` recommended

## Run
From this directory:
```bash
nvm use
npm install
npm run dev
```

## Verify
```bash
npm run lint
npm run build
```

## Data Source
Server components load JSON from:
- `../public_data/processed` (default when running from `web/`)
- `./public_data/processed` (fallback when running from repo root)
