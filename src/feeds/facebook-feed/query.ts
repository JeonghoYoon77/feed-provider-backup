function Query(limit: number): string {
	return `
SELECT
  ii.idx,
  ii.item_name,
  ii.item_description,
  ip.final_price,
  cud.product_no,
  ii.image_url,
  ii.brand_name
FROM cafe24_upload_db cud

JOIN item_info ii
ON cud.item_id = ii.idx

JOIN item_price ip
ON ii.idx = ip.item_id

WHERE ii.is_verify = 1
AND cud.is_active = 1
AND ii.item_priority > 0
ORDER BY ii.item_priority DESC
LIMIT ${limit};
`
}

export default Query
