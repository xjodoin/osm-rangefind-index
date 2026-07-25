// Append stale immutable object paths without turning the entire listing into
// function arguments. Large routing prefixes can contain hundreds of
// thousands of objects, which exceeds V8's argument-count limit when passed
// through `array.push(...paths)`.
export function appendStaleObjectPaths(target, objects, keep) {
  for (const object of objects) {
    if (object?.path && !keep.has(object.path)) target.push(object.path);
  }
  return target;
}
