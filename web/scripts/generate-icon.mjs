import satori from 'satori';
import { Resvg } from '@resvg/resvg-js';
import { readFileSync, writeFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

// Load fonts from @fontsource/alegreya-sans
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
        fontSize: 120,
        fontWeight: 500,
		paddingRight: 16,
      },
      children: [
        {
          type: 'span',
          props: {
            style: { fontStyle: 'italic', marginRight: 4 },
            children: 'my',
          },
        },
        {
          type: 'span',
          props: {
            style: {
              textDecoration: 'underline',
              textDecorationThickness: 2,
              marginRight: -8,
            },
            children: 'lib',
          },
        },
      ],
    },
  },
  {
    width: 256,
    height: 256,
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

const resvg = new Resvg(svg, {
  fitTo: { mode: 'width', value: 256 },
});

const pngData = resvg.render();
const pngBuffer = pngData.asPng();

const outputPath = join(__dirname, '..', 'public', 'icon-256.png');
writeFileSync(outputPath, pngBuffer);
console.log('Icon saved to', outputPath);
