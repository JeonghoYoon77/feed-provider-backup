import { GoogleSpreadsheet, GoogleSpreadsheetWorksheet } from 'google-spreadsheet'

import access from '../../../access.json'
import { FormProps } from './form'

const headers = [
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
  contents: FormProps[]
): Promise<void> {
  for (const content of contents) {
    await sheet.addRow({
      id: content.id,
      title: content.title,
      description: content.description,
      availability: content.availability,
      condition: content.condition,
      price: content.price,
      link: content.link,
      image_link: content.image_link,
      brand: content.brand,
    })
  }
}

export default Upload
