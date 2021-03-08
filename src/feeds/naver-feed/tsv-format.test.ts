import TSVFormat from './tsv-format'
import Constants from './constants'

const context = describe

describe('TSVFormat', () => {
	const constants = new Constants()
	const givenMainName: string = '스톤아일랜드'
	const givenFetchingCategoryName: string = '자켓'
	const givenItemName: string = '멤브라나 3L 더스트 후드 아노락'

	describe('title', () => {
		context('when gender is men and without custom color', () => {
			const givenCustomColor: string = ''
			const tsvFormat: TSVFormat = new TSVFormat({ itemGender: 'M' })
  
			it('returns includes text "남성"', () => {
				const title = tsvFormat.title({
					mainName: givenMainName,
					fetchingCategoryName: givenFetchingCategoryName,
					itemName: givenItemName,
					customColor: givenCustomColor
				})
  
				expect(title).toBe('스톤아일랜드 남성 자켓 멤브라나 3L 더스트 후드 아노락')
			})
		})
  
		context('when gender is women and with custom color', () => {
			const givenCustomColor: string = 'dark_green'
			const tsvFormat: TSVFormat = new TSVFormat({ itemGender: 'W' })
  
			it('returns includes text "여성" and with color', () => {
				const title = tsvFormat.title({
					mainName: givenMainName,
					fetchingCategoryName: givenFetchingCategoryName,
					itemName: givenItemName,
					customColor: givenCustomColor
				})
  
				expect(title).toBe('스톤아일랜드 여성 자켓 멤브라나 3L 더스트 후드 아노락 DARK GREEN')
			})
		})
	})

	describe('link', () => {
		const tsvFormat: TSVFormat = new TSVFormat({ itemGender: 'W' })
		const givenID: number = 1
    
		it('returns cafe24 link', () => {
			const link = tsvFormat.link({
				id: givenID,
				cafe24AddressPrefix: constants.cafe24AddressPrefix(),
			})

			expect(link).toBe(`${constants.cafe24AddressPrefix()}${givenID}`)
		})
	})
})
