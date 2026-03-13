import satori from 'satori';
import { Resvg } from '@resvg/resvg-js';
import { readFileSync, writeFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const publicDir = join(__dirname, '..', 'public');

const fontDir = join(__dirname, '..', 'node_modules', '@fontsource', 'alegreya-sans', 'files');
const fontData = readFileSync(join(fontDir, 'alegreya-sans-latin-500-normal.woff'));
const fontDataItalic = readFileSync(join(fontDir, 'alegreya-sans-latin-500-italic.woff'));

const svg = await satori(
  {
    type: 'div',
    props: {
      style: {
        width: '100%',
        height: '100%',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        backgroundColor: '#d4cfc4',
        fontFamily: 'Alegreya Sans',
        fontSize: 240,
        fontWeight: 500,
        paddingRight: 32,
      },
      children: [
        {
          type: 'span',
          props: {
            style: { fontStyle: 'italic', marginRight: 8 },
            children: 'my',
          },
        },
        {
          type: 'span',
          props: {
            style: {
              textDecoration: 'underline',
              textDecorationThickness: 4,
              marginRight: -16,
            },
            children: 'lib',
          },
        },
      ],
    },
  },
  {
    width: 512,
    height: 512,
    fonts: [
      {
        name: 'Alegreya Sans',
        data: fontData,
        weight: 500,
        style: 'normal',
      },
      {
        name: 'Alegreya Sans',
        data: fontDataItalic,
        weight: 500,
        style: 'italic',
      },
    ],
  }
);

const sizes = [
  { name: 'favicon-16x16.png', size: 16 },
  { name: 'favicon-32x32.png', size: 32 },
  { name: 'favicon-48x48.png', size: 48 },
  { name: 'apple-touch-icon.png', size: 180 },
  { name: 'android-chrome-192x192.png', size: 192 },
  { name: 'android-chrome-512x512.png', size: 512 },
  { name: 'mstile-150x150.png', size: 150 },
  { name: 'icon-256.png', size: 256 },
];

for (const { name, size } of sizes) {
  const resvg = new Resvg(svg, {
    fitTo: { mode: 'width', value: size },
  });
  const png = resvg.render().asPng();
  writeFileSync(join(publicDir, name), png);
  console.log(`Generated ${name}`);
}

const favicon32 = readFileSync(join(publicDir, 'favicon-32x32.png'));
writeFileSync(join(publicDir, 'favicon.ico'), favicon32);
console.log('Generated favicon.ico (32x32)');

const manifest = {
  name: 'mylib',
  short_name: 'mylib',
  icons: [
    { src: '/android-chrome-192x192.png', sizes: '192x192', type: 'image/png' },
    { src: '/android-chrome-512x512.png', sizes: '512x512', type: 'image/png' },
  ],
  theme_color: '#d4cfc4',
  background_color: '#d4cfc4',
  display: 'standalone',
};
writeFileSync(join(publicDir, 'site.webmanifest'), JSON.stringify(manifest, null, 2));
