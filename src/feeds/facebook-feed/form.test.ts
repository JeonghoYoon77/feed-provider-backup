import Form from './form'
import rows from './form.fixtures'

describe('Form', () => {
	it('returns converted facebook feed form', () => {
		const contents = Form(rows)
    
		expect(contents).toHaveLength(rows.length)

		contents.forEach((content, i: number) => {
			expect(content.id).toBe(Number(rows[i].idx))
			expect(content.title).toBe(rows[i]['item_name'])
			expect(content.description).toBe(rows[i]['item_description'])
			expect(content.price).toBe(rows[i]['final_price'] + 'KRW')
			expect(content.link).toBe('https://fetching.co.kr/product/detail.html?product_no=' + rows[i]['product_no'])
			expect(content['image_link']).toBe(rows[i]['image_url'])
			expect(content.brand).toBe(rows[i]['brand_name'])
		})
	})
})
