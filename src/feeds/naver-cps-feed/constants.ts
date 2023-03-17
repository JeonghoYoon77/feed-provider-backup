class Constants {
  private _limit: number = 100000
  private _address: string = 'https://fetching.co.kr/product/'
  private _condition: string = '신상품'
  private _shipping: number = 0
  private _includesVat: string = 'Y'
  private _eventWords: string = '#20만원 즉시 할인 #전상품 무료배송 #관부가세 포함 #2% 적립 #최대 80% 할인 #카드사별 2~8개월 무이자 혜택'
  private _eventWordsApp: string = '#20만원 즉시 할인 #전상품 무료배송 #관부가세 포함 #2% 적립 #최대 80% 할인 #카드사별 2~8개월 무이자 혜택'

  public limit(): number {
  	return this._limit
  }

  public address(): string {
  	return this._address
  }

  public condition(): string {
  	return this._condition
  }

  public shipping(): number {
  	return this._shipping
  }

  public includesVat(): string {
  	return this._includesVat
  }

  public eventWords(): string {
  	return this._eventWords
  }

  public eventWordsApp(): string {
  	return this._eventWordsApp
  }
}

export default Constants
