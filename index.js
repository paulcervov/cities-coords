const axios = require('axios').default;
const { pipeline } = require('stream/promises');
const csv = require('csv');
const fs = require('fs');
require('dotenv').config();

async function bootstrap() {

    const geocoderResponseCache = new Map();

    async function getCityCoordinates(city) {
        if(!geocoderResponseCache.has(city)) {
            const geocoderResponsePromise = axios(encodeURI(`https://geocode-maps.yandex.ru/1.x/?apikey=${process.env.API_KEY_GEOCODER}&format=json&geocode=${city}`));
            geocoderResponseCache.set(city, geocoderResponsePromise);
        }

        const { data: { response: { GeoObjectCollection: { featureMember } } } } = await geocoderResponseCache.get(city);
        const city_coordinates = featureMember?.[0].GeoObject.Point.pos;
        return { city_coordinates };
    }

    await pipeline(
        fs.createReadStream('centers_demo.csv'),
        csv.parse({
            columns: true,
            delimiter: ';',
            trim: true
        }),
        csv.transform((input, done) => {
            return getCityCoordinates(input.city)
                .then((resp) => {
                    return done(null, { ...input, ...resp })
                })
                .catch((err) => {
                    return done(err)
                })
        }),
        csv.stringify({header: true}),
        fs.createWriteStream('centers_demo_processed.csv')
    );
    console.log('Done 🍻');
}

bootstrap().catch(console.error);