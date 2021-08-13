import TSVFormat from './tsv-format'
import Constants from './constants'

const context = describe

describe('TSVFormat', () => {
	const constants = new Constants()
	const givenID: number = 1
	const givenMainName: string = '스톤아일랜드'
	const givenItemName: string = '멤브라나 3L 더스트 후드 아노락'
	const givenItemCode: string = 'STIL3LANO'

	describe('title', () => {
		context('when gender is men and without custom color', () => {
			const givenCustomColor: string = ''
			const tsvFormat: TSVFormat = new TSVFormat({ itemGender: 'M', id: givenID, shopId: 107, productNo: givenID })

			it('returns includes text "남성"', async () => {
				const title = await tsvFormat.title({
					shopId: 1,
					mainName: givenMainName,
					lastCategory: '가방',
					itemName: givenItemName,
					customColor: givenCustomColor,
					itemCode: givenItemCode
				})

				expect(title).toBe('스톤아일랜드 남성 멤브라나 3L 더스트 후드 아노락 STIL3LANO')
			})
		})

		context('when gender is women and with custom color', () => {
			const givenCustomColor: string = 'dark_green'
			const tsvFormat: TSVFormat = new TSVFormat({ itemGender: 'W', id: givenID, shopId: 107, productNo: givenID })

			it('returns includes text "여성" and with color', async () => {
				const title = await tsvFormat.title({
					shopId: 1,
					mainName: givenMainName,
					lastCategory: '가방',
					itemName: givenItemName,
					customColor: givenCustomColor,
					itemCode: givenItemCode
				})

				expect(title).toBe('스톤아일랜드 여성 멤브라나 3L 더스트 후드 아노락 STIL3LANO DARK GREEN')
			})
		})
	})

	describe('pcLink', () => {
		const tsvFormat: TSVFormat = new TSVFormat({ itemGender: 'W', id: givenID, shopId: 2, productNo: givenID })

		it('returns cafe24 link', () => {
			const link = tsvFormat.pcLink({
				cafe24PCAddress: constants.cafe24PCAddress(),
				cafe24PCAddressApp: constants.cafe24PCAddressApp(),
			})

			expect(link).toBe(`${constants.cafe24PCAddress()}${givenID}`)
		})
	})

	describe('mobileLink', () => {
		const tsvFormat: TSVFormat = new TSVFormat({ itemGender: 'W', id: givenID, shopId: 2, productNo: givenID })

		it('returns cafe24 link', () => {
			const link = tsvFormat.mobileLink({
				cafe24MobileAddress: constants.cafe24MobileAddress(),
				cafe24MobileAddressApp: constants.cafe24MobileAddressApp(),
			})

			expect(link).toBe(`${constants.cafe24MobileAddress()}${givenID}`)
		})
	})
})
