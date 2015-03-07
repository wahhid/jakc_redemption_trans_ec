<html>
<head>
    <style type="text/css">
    	${css}
    </style>
</head>
<body>
	<table border="1" width="100%">
		<tr>
			<th>Transaction ID</th>
			<th>Date</th>			
			<th>Customer</th>
			<th>Total Amount</th>
			<th>Total Item</th>
			<th>Total Coupon</th>
			<th>Total Point</th>																
		</tr>
<%
	total_amount = 0
	total_coupon = 0
	total_point = 0
%>

%for o in objects :
        <%
        	total_amount = total_amount + o.total_amount
        	total_coupon = total_coupon + o.total_coupon
        	total_point = total_point + o.total_point		
        %>
        <tr>
			<td style="font-size: 10px">${o.trans_id}</td>
			<td style="font-size: 10px">${o.trans_date}</td>
			<td style="font-size: 10px">${o.customer_id.name}</td>
			<td style="font-size: 10px">${o.total_amount}</td>
			<td style="font-size: 10px">${o.total_item}</td>
			<td style="font-size: 10px">${o.total_coupon}</td>
			<td style="font-size: 10px">${o.total_point}</td>
		</tr>
%endfor
		<tr>
			<td></td>
			<td></td>
			<td></td>
			<td><b>${total_amount}</b></td>
			<td></td>
			<td><b>${total_coupon}</b></td>
			<td><b>${total_point}</b></td>
		</tr>
</table>
</body>
</html>