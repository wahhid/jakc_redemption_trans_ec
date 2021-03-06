{
    'name' : 'Redemption and Point Management - Transaction Module',
    'version' : '1.0',
    'author' : 'JakC',
    'category' : 'Generic Modules/Redemption And Point Management',
    'depends' : ['base_setup','base','jakc_redemption','jakc_redemption_customer','jakc_redemption_customer_coupon','jakc_redemption_customer_point','jakc_redemption_tenant','jakc_redemption_trans_rule','jakc_redemption_schemas',],
    'init_xml' : [],
    'data' : [              			       
        'security/ir.model.access.csv',           
        'jakc_redemption_trans_view.xml',        
        'jakc_redemption_trans_menu.xml',  
        'jakc_redemption_customer_view.xml',
    ],
    'installable': True,
    'auto_install': False,
    'application': True,
}