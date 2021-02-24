import { RowDataPacket } from 'mysql2/promise'
import { parseInt } from 'lodash'

interface FormProps {
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
		const title: string = row['item_title']
		const description: string = row['item_description']
		const availability: string = 'available for order'
		const condition: string = 'new'
		const price: string = (<number>row['final_price']).toString() + 'KRW'
		const link: string = 'https://fetching.co.kr/product/detail.html?product_no='
      + (<number>row['product_no']).toString()
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
			'image_link': imageLink,
			brand,
		}
	})
	return contents
}

export default Form
