import { RowDataPacket } from 'mysql2/promise'
import { parseInt } from 'lodash'

export class Form {
  private id: number;
  private title: string;
  private description: string;
  private availability: string;
  private condition: string;
  private price: string;
  private link: string;
  private image_link: string;
  private brand: string;

	constructor(
		id: number,
		title: string,
		description: string,
		availability: string,
		condition: string,
		price: string,
		link: string,
		image_link: string,
		brand: string,
	) {
		this.id = id
		this.title = title
		this.description = description
		this.availability = availability
		this.condition = condition
		this.price = price
		this.link = link
		this.image_link = image_link
		this.brand = brand
	}

	toObject() {
		return {
			id: this.id,
			title: this.title,
			description: this.description,
			availability: this.availability,
			condition: this.condition,
			price: this.price,
			link: this.link,
			image_link: this.image_link,
			brand: this.brand,
		}
	}
}

function Format(rows: RowDataPacket[]): Form[] {
	const contents = rows.map((row: RowDataPacket): Form => {
		const id: number = parseInt(row.idx)
		const title: string = row['item_name']
		const description: string = row['item_description']
		const availability: string = 'available for order'
		const condition: string = 'new'
		const price: string = makePrice(<number>row['final_price'])
		const link: string = makeLink(<number>row['product_no'])
		const imageLink: string = row['image_url']
		const brand: string = row['brand_name']

		return new Form(
			id,
			title,
			description,
			availability,
			condition,
			price,
			link,
			imageLink,
			brand,
		)
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

export default Format
