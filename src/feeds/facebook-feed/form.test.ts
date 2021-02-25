import Format, { Form } from './form'
import rows from './form.fixtures'

describe('Form', () => {
	it('returns converted facebook feed form', () => {
		const contents = Format(rows)
    
		expect(contents).toHaveLength(rows.length)

		contents.forEach((content: Form, i: number) => {
			const actual = content.toObject();
			
			expect(actual.id).toBe(Number(rows[i].idx))
			expect(actual.title).toBe(rows[i]['item_name'])
			expect(actual.description).toBe(rows[i]['item_description'])
			expect(actual.price).toBe(rows[i]['final_price'] + 'KRW')
			expect(actual.link).toBe('https://fetching.co.kr/product/detail.html?product_no=' + rows[i]['product_no'])
			expect(actual['image_link']).toBe(rows[i]['image_url'])
			expect(actual.brand).toBe(rows[i]['brand_name'])
		})
	})
})
