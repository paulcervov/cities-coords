const axios = require('axios').default;
const { pipeline } = require('stream/promises');
const csv = require('csv');
const fs = require('fs');
require('dotenv').config();

async function bootstrap() {

    await pipeline(
        fs.createReadStream('centers_demo.csv'),
        csv.parse({
            columns: true,
            delimiter: ';',
            trim: true
        }),
        csv.transform(async (input, done) => {
            const url = encodeURI(`https://geocode-maps.yandex.ru/1.x/?apikey=${process.env.API_KEY_GEOCODER}&format=json&geocode=${input.city}`);
            const {data: {response: {GeoObjectCollection: {featureMember}}}} = await axios(url);
            const city_coordinates = featureMember[0].GeoObject.Point.pos;
            done(null, {...input, city_coordinates})
        }),
        csv.stringify({header: true}),
        fs.createWriteStream('centers_demo_processed.csv')
    );
    console.log('Done 🍻');
}

bootstrap().catch(console.error);