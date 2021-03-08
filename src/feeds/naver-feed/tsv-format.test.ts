import TSVFormat from './tsv-format'

const context = describe

describe('TSVFormat', () => {
	const givenMainName: string = '스톤아일랜드'
	const givenFetchingCategoryName: string = '자켓'
	const givenItemName: string = '멤브라나 3L 더스트 후드 아노락'

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
