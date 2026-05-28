const USE_MOCK_DATA = import.meta.env.VITE_USE_MOCK_DATA === "true";

async function getMockJson(fileName, label) {
  if (!USE_MOCK_DATA) {
    throw new Error(`${label} mock data is disabled in production mode`);
  }
  const response = await fetch(["", "mock", fileName].join("/"));
  if (!response.ok) throw new Error(`Failed to load ${label}`);
  return response.json();
}

export async function getRealtimeOpenAQ() {
  return getMockJson("realtime-openaq.json", "realtime OpenAQ");
}

export async function getRealtimeWeather() {
  return getMockJson("realtime-weather.json", "realtime weather");
}

export async function getHistoricalOpenAQ() {
  return getMockJson("historical-openaq.json", "historical OpenAQ");
}

export async function getHistoricalWeather() {
  return getMockJson("historical-weather.json", "historical weather");
}

export async function getHistoricalSentinel() {
  return getMockJson("historical-sentinel.json", "historical Sentinel-5P");
}
