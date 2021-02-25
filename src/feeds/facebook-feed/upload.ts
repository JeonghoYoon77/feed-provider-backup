import { GoogleSpreadsheet, GoogleSpreadsheetWorksheet } from 'google-spreadsheet'

import info from '../../../access.json'
import { FormProps } from './form'

const SPREACSHEET_ID = '1LbkUtiVKm48vA1SAxbqPtZ2WOhqwJKOkw9-JPIh8jvA'

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
  const doc: GoogleSpreadsheet = new GoogleSpreadsheet(SPREACSHEET_ID);
  
  await doc.useServiceAccountAuth({
    client_email: info.client_email,
    private_key: info.private_key,
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
