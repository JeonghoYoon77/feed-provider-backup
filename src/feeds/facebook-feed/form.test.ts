import Form from './form'
import rows from './form.fixtures'

describe('Form', () => {
	it('returns converted facebook feed form', () => {
		const contents = Form(rows)
    
		expect(contents).toHaveLength(rows.length)
		expect(contents[0].id).toBe(Number(rows[0].idx))
		expect(contents[0].title).toBe(rows[0]['item_title'])
		expect(contents[0].description).toBe(rows[0]['item_description'])
		expect(contents[0].price).toBe(rows[0]['final_price'] + 'KRW')
		expect(contents[0].link).toBe('https://fetching.co.kr/product/detail.html?product_no=' + rows[0]['product_no'])
		expect(contents[0]['image_link']).toBe(rows[0]['image_url'])
		expect(contents[0].brand).toBe(rows[0]['brand_name'])
	})
})
