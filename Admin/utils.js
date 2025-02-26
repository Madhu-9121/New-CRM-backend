exports.extractFullNames = (inputString) => {
  try{
    const fullNamePattern = /\b[A-Z][a-zA-Z\s]+\b/g;
    const matches = inputString.match(fullNamePattern);
    return matches.filter(value => value?.toLowerCase() !== 'mr' && value?.toLowerCase() !== 'mrs') || [];
  }catch(e){
    console.log('Failed foor ',inputString)
    return []
  }

}

exports.designations = [
  "MANAGER",
  "SENIOR MANAGER",
  "EXPORT MANAGER",
  "ADDITIONAL DIRECTOR",
  "VICE PRESIDEN",
  "WHOLETIME DIRECTOR",
  "FINANCE",
  "HEAD MERCHANDISER",
  "MANAGER",
  "MANAGING",
  "MD",
  "CEO",
  "FINANCIER",
  "OWNER",
  "FINANCE MANAGER",
  "PARTNER",
  "ACCOUNTANT",
  "DIRECTOR",
  "CHAIRMAN&CEO",
  "DESIGNATED PARTNER",
  "FINANCE MANAGER ",
  "OVERSEA MANAGER",
  "MANAGING DIRECTOR",
  "DIRECTOR"
]

exports.convertStringToList = (inputString) => {
  let result = []
  for(let i =0; i < inputString.length ; i+=2){
    result.push(`${inputString.charAt(i)}${inputString.charAt(i+1)}`)
  }
  return result
}

exports.getLatestDate = (date1, date2) => {
  if(!date1){
    return date2
  }
  if(!date2){
    return date1
  }
  return new Date(date1).getTime() > new Date(date2).getTime() ? date1 : date2;
}

exports.escapeRegExp = (string) => {
  return string.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

exports.modifyCompanyString = (inputString)  => {
  // Remove any dots "."
  let modifiedString = inputString?.replace(/\./g, "")?.toUpperCase();
  
  // Replace "Private Limited" with "PVT LTD"
  modifiedString = modifiedString?.replace(/PRIVATE LIMITED/g, "PVT LTD");
  
  // Replace "Limited" with "LTD"
  modifiedString = modifiedString?.replace(/LIMITED/g, "LTD");

  return modifiedString?.toUpperCase();
}

exports.LCPurposeObject = {
  "lc_discounting": "LC discounting (International)",
  "lc_confirmation": "LC confirmation (International)",
  "lc_confirmation_discounting": "LC Confirmation & Discounting (International)",
  "lc_discounting_domestic": "LC discounting (Domestic)",
  "lc_confirmation_domestic": "LC confirmation (Domestic)",
  "lc_confirmation_discounting_domestic": "LC Confirmation & Discounting (Domestic)",
  "sblc": "SBLC"
}