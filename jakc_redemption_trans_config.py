from openerp.osv import fields, osv
import logging

_logger = logging.getLogger(__name__)

class rdm_trans_config(osv.osv):
    _name = 'rdm.trans.config'
    _description = 'Redemption Trans Config'
    
    def get_config(self, cr, uid, context=None):
        ids = self.search(cr, uid, [('state','=', True),], context=context)
        if ids:
            return self.pool.get('rdm.trans.config').browse(cr, uid, ids[0], context=context)
        else:
            return None
            
    _columns = {
        'trans_delete_allowed': fields.boolean('Allow Delete Transaction'),
        'trans_delete_approver': fields.integer('Delete Transaction Approver'),
        'trans_email_tmpl': fields.integer('Transaction Email'),
        'state': fields.boolean('Status'),
    }
    _defaults = {
        'state': lambda *a: True,        
    }
rdm_trans_config()

class rdm_trans_config_settings(osv.osv_memory):
    _name = 'rdm.trans.config.settings'
    _inherit = 'res.config.settings'
    _columns = {
        'trans_delete_allowed': fields.boolean('Allow Delete Transaction'),
        'trans_delete_approver': fields.many2one('hr.employee','Delete Transaction Approver'),
        'trans_email_tmpl': fields.many2one('email.template','Transaction Email'),
        'state': fields.boolean('Status'),
    }            
    
    def _get_config(self, cr, uid, context=None):
        ids = self.pool.get('rdm.trans.config').search(cr, uid, [('state','=', True),], context=context)
        if ids:
            config  = self.pool.get('rdm.trans.config').browse(cr, uid, ids[0], context=context)
        else:
            config = None
        return config
    
        
    def get_default_trans_delete_allowed(self, cr, uid, fields, context=None):
        config = self._get_config(cr, uid, context)
        if config:
            trans_delete_allowed = config.trans_delete_allowed
        else: 
            data = {}
            result_id = self.pool.get('rdm.trans.config').create(cr, uid, data, context=context)
            trans_delete_allowed = False
        return {'trans_delete_allowed': trans_delete_allowed}

    def set_default_trans_delete_allowed(self, cr, uid, ids, context=None):
        config = self._get_config(cr, uid, context)
        setting = self.browse(cr, uid, ids[0], context)
        trans_delete_allowed = setting.trans_delete_allowed
        self.pool.get('rdm.trans.config').write(cr, uid, [config.id], {'trans_delete_allowed': trans_delete_allowed})  
        
    def get_default_trans_delete_approver(self, cr, uid, fields, context=None):
        config = self._get_config(cr, uid, context)
        if config:
            trans_delete_approver = config.trans_delete_approver
        else: 
            data = {}
            result_id = self.pool.get('rdm.trans.config').create(cr, uid, data, context=context)
            trans_delete_approver = None
        return {'trans_delete_approver': trans_delete_approver}

    def set_default_trans_delete_approver(self, cr, uid, ids, context=None):
        config = self._get_config(cr, uid, context)
        setting = self.browse(cr, uid, ids[0], context)
        trans_delete_approver = setting.trans_delete_approver
        self.pool.get('rdm.trans.config').write(cr, uid, [config.id], {'trans_delete_approver': trans_delete_approver})   
        
    def get_default_trans_email_tmpl(self, cr, uid, fields, context=None):    
        ids = self.pool.get('rdm.trans.config').search(cr, uid, [('state','=', True),], context=context)
        if ids:
            trans_email_tmpl = self.pool.get('rdm.trans.config').browse(cr, uid, ids[0], context=context).trans_email_tmpl
        else:
            customer_data = {}
            result_id = self.pool.get('rdm.trans.config').create(cr, uid, customer_data, context=context)
            trans_email_tmpl = 0
        return {'trans_email_tmpl': trans_email_tmpl}
    
    def set_default_trans_email_tmpl(self, cr, uid, ids, context=None):
        config_ids = self.pool.get('rdm.trans.config').search(cr, uid, [('state','=', True),], context=context)
        config = self.browse(cr, uid, ids[0], context)
        trans_email_tmpl=config.trans_email_tmpl
        self.pool.get('rdm.trans.config').write(cr, uid, config_ids, {'trans_email_tmpl': trans_email_tmpl}) 