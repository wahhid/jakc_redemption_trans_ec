from openerp.osv import fields, osv

class rdm_customer(osv.osv):
    _name = "rdm.customer"
    _inherit = "rdm.customer"
    _columns = {
        'promo_trans_ids': fields.one2many('rdm.trans','customer_id','Promo',readonly=True),
        'point_trans_ids': fields.one2many('rdm.trans','customer_id','Point',readonly=True)
    }
rdm_customer()