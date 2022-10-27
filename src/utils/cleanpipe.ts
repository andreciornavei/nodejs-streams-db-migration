// <!-- remove pipes from a value -->
export const cleanpipe = (value: any): string => {
  return String(value).replace(/\|/g, "");
};
