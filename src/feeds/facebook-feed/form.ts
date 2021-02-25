import { RowDataPacket } from 'mysql2/promise'
import { parseInt } from 'lodash'

export interface FormProps {
  id: number;
  title: string;
  description: string;
  availability: string;
  condition: string;
  price: string;
  link: string;
  'image_link': string;
  brand: string;
}

function Form(rows: RowDataPacket[]): FormProps[] {
	const contents = rows.map((row: RowDataPacket): FormProps => {
		const id: number = parseInt(row.idx)
		const title: string = row['item_name']
		const description: string = row['item_description']
		const availability: string = 'available for order'
		const condition: string = 'new'
		const price: string = makePrice(<number>row['final_price'])
		const link: string = makeLink(<number>row['product_no'])
		const imageLink: string = row['image_url']
		const brand: string = row['brand_name']

		return {
			id,
			title,
			description,
			availability,
			condition,
			price,
			link,
			image_link: imageLink,
			brand,
		}
	})
	return contents
}

function makePrice(finalPrice: number): string {
	const currency = 'KRW'
	return finalPrice.toString() + currency
}

function makeLink(productNo: number): string {
	const fetchingCafe24URL = 'https://fetching.co.kr/product/detail.html?product_no='
	return fetchingCafe24URL + productNo.toString()
}

export default Form
