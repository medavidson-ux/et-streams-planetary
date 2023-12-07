const fs = require("fs");
const csv = require("csvtojson");
// const {promisify} = require('util');
const { Transform, pipeline } = require("stream");
const { transformOnePlanet } = require("./transform.js");

const inputStream = fs.createReadStream("data/planetRaw.csv");
const outputStream = fs.createWriteStream("data/outStreaming.ndjson");

const csvParser = csv();

const transformPlanetStream = new Transform({
  transform: function (planet, encoding, cb) {
    //transform the input
    try {
      const planetObject = JSON.parse(planet);
      const transformedPlanet = transformOnePlanet(planetObject);
      const planetString = `${JSON.stringify(transformedPlanet)}` + "\n";
      cb(null, planetString);
    } catch (err) {
      cb(err);
    }
  },
});

// inputStream.pipe(csvParser).pipe(transformPlanetStream).pipe(outputStream);

// inputStream.on("error", (err) => console.log(err));
// inputStream.on("error", (err) => console.log(err));
// inputStream.on("error", (err) => console.log(err));
// inputStream.on("error", (err) => console.log(err));

function handleError(err) {
  if (err) {
    console.log("Error occurred in pipeline:", err);
  } else {
    console.log("Pipeline completed successfully.");
  }
}

pipeline(
  inputStream,
  csvParser,
  transformPlanetStream,
  outputStream,
  handleError
);
