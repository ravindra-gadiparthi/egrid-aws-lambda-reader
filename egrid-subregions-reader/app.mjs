import { pointToTile } from '@mapbox/tilebelt';
import booleanContains from '@turf/boolean-contains';
import flatten from '@turf/flatten';
import AWS from 'aws-sdk';
import zlib from 'zlib';

const s3 = new AWS.S3({});

const s3Bucket = 'egrid-subregions-cloudcafe'

export const lambdaHandler = async (event, context) => {

    try {
        console.log(JSON.stringify(event));
        let {long, lat , year, zoom} = JSON.parse(event.body);
        const responseBody = await lookupS3(long, lat , year, zoom);
        return {
            'statusCode': 200,
            'body': responseBody
        }
    } catch (err) {
        console.log(err);
        return err;
    }
};

async function lookupS3(lon, lat, year, zoom) {
    const start = Date.now();
    const key = pointToTile(lon, lat, zoom).join('-');
    const zip_file_path = `${year}_gzip/${key}.json.zip`;
    const file_path = `${year}/${key}.json`;
    console.log(zip_file_path);

    try {
        const compressedData = await fetchJson(s3Bucket,zip_file_path);
        //const uncompressedData = await fetchJson(s3Bucket,file_path);
        const uncompressedData = zlib.inflateSync(compressedData);
        const features = JSON.parse(uncompressedData);
        for (const feature of features) {
            const point = {
                type: 'Point',
                coordinates: [ lon, lat ]
            };
            for (const poly of flatten(feature).features) {
                if (booleanContains(poly, point)) {
                    console.log(`eGRID Subregion: ${JSON.stringify(feature.properties)}`);
                    const end = Date.now();
                    console.log("total execution time is on "+(end-start)/1000);
                    return JSON.stringify(feature.properties);  
                }
            }
        }
    } catch (e) {
        if (e.code === 'ENOENT') {
            console.log('Not found');
        } else {
            console.error(e);
        }
    }
    return {};
}

function fetchJson(bucket, key) {
    return new Promise((resolve, reject) => {
      s3.getObject({ Bucket: bucket, Key: key }, (err, data) => {
        if (err) return reject(err)
        try {
          resolve(data.Body)
        } catch (e) {
          reject(err)
        }
      })
    })
  }