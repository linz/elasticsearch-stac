import { Client } from '@elastic/elasticsearch';
import { S3Fs } from '@linzjs/s3fs';
import PLimit from 'p-limit';
import pino from 'pino';
import { PrettyTransform } from 'pretty-json-log';
import { OnDropDocument } from '@elastic/elasticsearch/lib/Helpers';

import dotenv from 'dotenv';
dotenv.config();

const SourceLocation = process.env['S3_SOURCE_BUCKET'];
const TargetIndex = process.env['ELASTIC_INDEX'];

const logger = process.stdout.isTTY ? pino(PrettyTransform.stream()) : pino();

const fs = new S3Fs();

const connection = {
  id: process.env['ELASTIC_ID'] ?? '',
  username: process.env['ELASTIC_USERNAME'] ?? '',
  password: process.env['ELASTIC_PASSWORD'] ?? '',
};
if (connection.id === '' || connection.username === '' || connection.password === '') {
  throw new Error('Missing $ELASTIC_ env setup');
}

const Q = PLimit(25);

interface StacFile {
  _id: string;
  '@source': string;
  '@timestamp': string;
  stac_version: string;
  stac_extensions: string[];
  id: string;
  geometry: { type: string; coordinates: number[] };
  links: { rel: string; href: string }[];
  assets?: Record<string, { href: string; type: string; roles: string[] }>;
  properties?: {
    datetime: string;
  };
  summaries?: {
    'linz:generated': {
      date: string;
      datetime: string;
    }[];
  };
}

async function readJson<T>(file: string): Promise<T | null> {
  logger.trace({ file }, 'File:Read');
  const content = await fs.read(file);
  try {
    return JSON.parse(content.toString());
  } catch (e) {}
  return null;
}

async function processFile(file: string): Promise<StacFile | null> {
  const json = await readJson<StacFile>(file);
  if (json == null) return null;
  if (json.stac_version == null) return null;
  logger.debug({ file, fileId: json.id }, 'StacFile');

  if (json.summaries) {
    json['@timestamp'] = json.summaries['linz:generated'][0].date ?? json.summaries['linz:generated'][0].datetime;
  } else if (json.properties) {
    json['@timestamp'] = json.properties.datetime;
  }

  if (json['@timestamp'] == null) {
    logger.error({ file }, 'File:Timestamp:Missing');
  }

  const links = json.links.filter((f) => f.rel === 'self');
  if (links.length !== 1) {
    console.log('Weird Links', { file, links });
  } else {
    if (links[0].href !== file) {
      logger.info({ file, oldLink: links[0].href }, 'File:CorrectLinkSelf');
      links[0].href = file;
    }
  }

  if (json.assets) {
    const sourceFolder = file.substr(0, file.lastIndexOf('/'));
    for (const asset of Object.values(json.assets)) {
      if (asset.href.startsWith('s3://') || asset.href.startsWith('https://')) continue;

      const newLink = fs.join(sourceFolder, asset.href);
      logger.info({ file, oldLink: asset.href, newLink }, 'File:CorrectAssetHref');
      asset.href = newLink;
    }
  }

  json['@source'] = file;
  return json;
}

async function main(): Promise<void> {
  if (SourceLocation == null) throw new Error('Missing $S3_SOURCE_BUCKET');
  if (TargetIndex == null) throw new Error('Missing $ELASTIC_INDEX');

  const todo: Promise<StacFile | null>[] = [];
  for await (const file of fs.list(SourceLocation)) {
    if (!file.endsWith('.json')) continue;
    todo.push(Q(() => processFile(file)));
  }

  const stacs = await Promise.all(todo);

  const elastic = new Client({
    cloud: { id: connection.id },
    auth: { username: connection.username, password: connection.password },
  });

  await elastic.helpers.bulk({
    datasource: stacs.filter((f) => f != null) as StacFile[],
    onDocument(f: StacFile) {
      return {
        index: { _index: TargetIndex, _id: f['@source'] },
      };
    },
    onDrop(err: OnDropDocument<StacFile>) {
      logger.error({ logMessage: JSON.stringify(err.document), error: err.error }, 'FailedIndex');
    },
  });
}

main();
