import { GoogleSpreadsheet, GoogleSpreadsheetWorksheet } from 'google-spreadsheet'
import { slice } from 'lodash'

import access from '../../../facebook_spreadsheet_access.json'
import { Form } from './form'

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

async function Upload(contents: Form[]): Promise<void> {
	const sheet: GoogleSpreadsheetWorksheet = await getSheet()
	await sheet.clear()
	await sheet.setHeaderRow(headers)
	await setContents(sheet, contents)
}

async function getSheet(): Promise<GoogleSpreadsheetWorksheet> {
	const doc: GoogleSpreadsheet = new GoogleSpreadsheet(access.spreadsheet_id)

	await doc.useServiceAccountAuth({
		client_email: access.client_email,
		private_key: access.private_key,
	})

	await doc.loadInfo()

	return doc.sheetsByIndex[0]
}

async function setContents(
	sheet: GoogleSpreadsheetWorksheet,
	contents: Form[],
	index: number | undefined = 0,
): Promise<void> {
	if (contents.length < index) {
		return
	}

	const sliceSize = 1000
	const nextIndex = index + sliceSize
	const currentContents = slice(contents, index, nextIndex)
	const sheetContents = currentContents.map((content: Form) => content.toObject())

	await sheet.addRows(sheetContents)
	console.log(`upload rows ${index} ~ ${nextIndex}`)

	return setContents(sheet, contents, nextIndex)
}

export default Upload
