const axios = require('axios').default;
const { pipeline } = require('stream/promises');
const csv = require('csv');
const fs = require('fs');
require('dotenv').config();

async function bootstrap() {

    const cityCoordinatesCache = new Map();

    await pipeline(
        fs.createReadStream('centers_demo.csv'),
        csv.parse({
            columns: true,
            delimiter: ';',
            trim: true
        }),
        csv.transform(async (input, done) => {

            // try to get coordinates from cache
            if(cityCoordinatesCache.has(input.city)){
                done(null, {...input, city_coordinates: cityCoordinatesCache.get(input.city)})
            }

            // request to geocoder API
            const url = encodeURI(`https://geocode-maps.yandex.ru/1.x/?apikey=${process.env.API_KEY_GEOCODER}&format=json&geocode=${input.city}`);
            const {data: {response: {GeoObjectCollection: {featureMember}}}} = await axios(url);
            const city_coordinates = featureMember[0].GeoObject.Point.pos;

            // update cache
            cityCoordinatesCache.set(input.city, city_coordinates);

            done(null, {...input, city_coordinates})
        }),
        csv.stringify({header: true}),
        fs.createWriteStream('centers_demo_processed.csv')
    );
    console.log('Done 🍻');
}

bootstrap().catch(console.error);