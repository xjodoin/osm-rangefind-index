function normalizeLongitude(value) {
  const normalized = ((value + 180) % 360 + 360) % 360 - 180;
  return Object.is(normalized, -0) ? 0 : normalized;
}

function roundCoordinate(value) {
  const rounded = Math.round(value * 1e6) / 1e6;
  return Object.is(rounded, -0) ? 0 : rounded;
}

function collectPositions(value, positions) {
  if (!Array.isArray(value)) return;
  if (
    value.length >= 2
    && Number.isFinite(Number(value[0]))
    && Number.isFinite(Number(value[1]))
  ) {
    positions.push([Number(value[0]), Number(value[1])]);
    return;
  }
  for (const child of value) collectPositions(child, positions);
}

// Return [minLat, minLon, maxLat, maxLon]. A wrapped longitude interval is
// represented by minLon > maxLon, which avoids turning dateline-spanning
// regions into almost-global routing boxes.
export function geometryCoverageBbox(geometry) {
  const positions = [];
  collectPositions(geometry?.coordinates, positions);
  if (!positions.length) return null;

  let minLat = Infinity;
  let maxLat = -Infinity;
  const longitudes = [];
  for (const [rawLon, lat] of positions) {
    if (lat < -90 || lat > 90) continue;
    minLat = Math.min(minLat, lat);
    maxLat = Math.max(maxLat, lat);
    longitudes.push(normalizeLongitude(rawLon));
  }
  if (!longitudes.length) return null;

  longitudes.sort((a, b) => a - b);
  let largestGap = -1;
  let gapIndex = 0;
  for (let index = 0; index < longitudes.length; index++) {
    const next = index + 1 < longitudes.length
      ? longitudes[index + 1]
      : longitudes[0] + 360;
    const gap = next - longitudes[index];
    if (gap > largestGap) {
      largestGap = gap;
      gapIndex = index;
    }
  }

  const coveredLongitude = 360 - largestGap;
  const minLon = coveredLongitude >= 359.999999
    ? -180
    : normalizeLongitude(longitudes[(gapIndex + 1) % longitudes.length]);
  const maxLon = coveredLongitude >= 359.999999
    ? 180
    : normalizeLongitude(longitudes[gapIndex]);

  return [
    roundCoordinate(minLat),
    roundCoordinate(minLon),
    roundCoordinate(maxLat),
    roundCoordinate(maxLon)
  ];
}
