import { GoogleSpreadsheet, GoogleSpreadsheetWorksheet } from 'google-spreadsheet'
import { slice } from 'lodash'

import access from '../../../facebook_spreadsheet_access.json'
import { FormProps } from './form'

const headers: string[] = [
  'id',
  'title',
  'description',
  'availability',
  'condition',
  'price',
  'link',
  'image_link',
  'brand',
]

async function Upload(contents: FormProps[]): Promise<void> {
  const sheet: GoogleSpreadsheetWorksheet = await getSheet()
  await sheet.clear()
  await sheet.setHeaderRow(headers)
  await setContents(sheet, contents)
}

async function getSheet(): Promise<GoogleSpreadsheetWorksheet> {
  const doc: GoogleSpreadsheet = new GoogleSpreadsheet(access.spreadsheet_id);
  
  await doc.useServiceAccountAuth({
    client_email: access.client_email,
    private_key: access.private_key,
  });

  await doc.loadInfo()

  return doc.sheetsByIndex[0];
}

async function setContents(
  sheet: GoogleSpreadsheetWorksheet, 
  contents: FormProps[],
  index: number | undefined = 0,
): Promise<void> {
  if (contents.length < index) {
    return
  }

  const sheetContents = contents.map((content: FormProps) => ({
    id: content.id,
    title: content.title,
    description: content.description,
    availability: content.availability,
    condition: content.condition,
    price: content.price,
    link: content.link,
    image_link: content.image_link,
    brand: content.brand,
  }))
  const sliceSize = 1000
  const nextIndex = index + sliceSize
  const currentSheetContents = slice(sheetContents, index, nextIndex)

  await sheet.addRows(currentSheetContents)
  console.log(`insert rows ${index} ~ ${nextIndex}`)

  return setContents(sheet, contents, nextIndex)
}

export default Upload
