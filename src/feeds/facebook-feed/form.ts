import { RowDataPacket } from 'mysql2/promise'
import { parseInt, capitalize, isEmpty } from 'lodash'

export class Form {
  private id: number;
  private title: string;
  private description: string;
  private availability: string = 'available for order';
  private condition: string = 'new';
	private currency: string = 'KRW';
  private price: number;
	private linkPrefix: string = 'https://fetching.co.kr/product/detail.html?product_no=';
  private productNo: number;
  private image_link: string;
  private brand: string;

	constructor(
		id: number,
		title: string,
		description: string,
		price: number,
		productNo: number,
		image_link: string,
		brand: string,
	) {
		this.id = id
		this.title = title
		this.description = description
		this.price = price
		this.productNo = productNo
		this.image_link = image_link
		this.brand = brand
	}

	toObject() {
		return {
			id: this.id,
			title: capitalize(this.title),
			description: isEmpty(this.description) ? this.title : this.description,
			availability: this.availability,
			condition: this.condition,
			price: `${this.price}${this.currency}`,
			link: `${this.linkPrefix}${this.productNo}`,
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
		const price: number = <number>row['final_price']
		const productNo: number = <number>row['product_no']
		const imageLink: string = row['image_url']
		const brand: string = row['brand_name']

		return new Form(
			id,
			title,
			description,
			price,
			productNo,
			imageLink,
			brand,
		)
	})
	return contents
}

export default Format
