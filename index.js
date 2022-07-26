const axios = require('axios').default;
const { pipeline } = require('stream/promises');
const csv = require('csv');
const fs = require('fs');
require('dotenv').config();

async function bootstrap() {

    const cityCoordsCache = new Map();

    await pipeline(
        fs.createReadStream('centers_demo.csv'),
        csv.parse({
            columns: true,
            delimiter: ';',
            trim: true
        }),
        csv.transform(async (input, done) => {
            if(!cityCoordsCache.has(input.city)) {
                console.log(`First callback, get ${input.city} coords from geocoder...`);
                const url = encodeURI(`https://geocode-maps.yandex.ru/1.x/?apikey=${process.env.API_KEY_GEOCODER}&format=json&geocode=${input.city}`);
                const {data: {response: {GeoObjectCollection: {featureMember}}}} = await axios(url);
                const city_coordinates = featureMember[0].GeoObject.Point.pos;
                cityCoordsCache.set(input.city, city_coordinates);
            }
            console.log(`First callback, coords for ${input.city}: `, cityCoordsCache.get(input.city))
            done(null, {...input, city_coordinates: cityCoordsCache.get(input.city)});
        }),
        csv.transform(async (input, done) => {
            console.log(`Second callback, coords for ${input.city}: `, cityCoordsCache.get(input.city));
            done(null, input)
        }),
        csv.stringify({header: true}),
        fs.createWriteStream('centers_demo_processed.csv')
    );
    console.log('Done 🍻');
}

bootstrap().catch(console.error);