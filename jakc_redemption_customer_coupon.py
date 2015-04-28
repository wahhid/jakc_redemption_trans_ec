from openerp.osv import fields, osv

class rdm_customer_coupon(osv.osv):
    _name = "rdm.customer.coupon"
    _inherit = "rdm.customer.coupon"
    _columns = {
        'trans_id': fields.many2one('rdm.trans','Transaction ID',readonly=True),        
    }
rdm_customer_coupon()