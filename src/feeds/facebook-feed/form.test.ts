import Form from './form'
import rows from './form.fixtures'

describe('Form', () => {
	it('', () => {
		const contents = Form(rows)
    
		expect(contents).toHaveLength(rows.length)
		expect(contents[0].id).toBe(Number(rows[0].idx))
	})
})
