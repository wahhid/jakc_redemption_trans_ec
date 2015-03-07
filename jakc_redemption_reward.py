from openerp.osv import fields, osv

class rdm_reward_trans(osv.osv):
    _name = "rdm.reward.trans"
    _inherit = "rdm.reward.trans"
    _columns = {
        'trans_id': fields.many2one('rdm.trans','Transaction',readonly=True)
    }
rdm_reward_trans()