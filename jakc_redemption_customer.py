from openerp.osv import fields, osv

class rdm_customer(osv.osv):
    _name = "rdm.customer"
    _inherit = "rdm.customer"
    _columns = {
        'trans_ids': fields.one2many('rdm.trans','customer_id','Transaction',readonly=True),        
    }
rdm_customer()