function query(limit: number): string {
	return `
SELECT
  ii.idx,
  ii.item_name,
  ii.item_description,
  ip.final_price,
  cud.product_no,
  ii.image_url,
  ii.brand_name
FROM cafe24_upload_list cul

JOIN cafe24_upload_db cud
ON cul.item_id = cud.item_id

JOIN item_info ii
ON cul.item_id = ii.idx

JOIN item_price ip
ON ii.idx = ip.item_id

WHERE ii.is_verify = 1
AND cul.is_naver_upload = 1
LIMIT ${limit};
`
}

export default query
