from openerp.osv import fields, osv
import datetime
from decimal import *
import logging
from openerp.netsvc import _logger_init
_logger = logging.getLogger(__name__)

AVAILABLE_STATES = [
    ('draft','New'),
    ('request','Request'),
    ('ready','Ready'),    
    ('open','Open'),  
    ('calculate','Calculate'),  
    ('done', 'Closed'),
    ('req_delete','Request For Delete'),
    ('delete','Deleted'),
]

reportserver = '172.16.0.3'
reportserverport = '8080'

class rdm_trans_receipt_report(osv.osv_memory):
    _name = "rdm.trans.receipt.report"
    _columns = {
        'id' : fields.integer('ID', required=True),        
    } 
        
    def generate_report(self, cr, uid, ids, context=None):
        rdm_config = self.pool.get('rdm.config').get_config(cr, uid, context)
        params = self.browse(cr, uid, ids, context=context)
        param = params[0]   
        serverUrl = 'http://' + rdm_config.report_server + ':' + rdm_config.report_server_port +'/jasperserver'
        ParentFolderUri = '/rdm'
        reportUnit = '/rdm/rdm_trans_receipt_report'
        url = serverUrl + '/flow.html?_flowId=viewReportFlow&standAlone=true&_flowId=viewReportFlow&ParentFolderUri=' + ParentFolderUri + '&reportUnit=' + reportUnit + '&ID=' +  param.id + '&decorate=no&j_username=' + rdm_config.report_user + '&j_password=' + rdm_config.report_password
        return {
            'type':'ir.actions.act_url',
            'url': url,
            'nodestroy': True,
            'target': 'new' 
        }
        
rdm_trans_receipt_report()


class rdm_trans(osv.osv):
    _name =  "rdm.trans"
    _description = "Redemption Transaction"
    
    def trans_close(self, cr, uid, ids, context=None):
        values = {}
        values.update({'state':'done'})
        self.write(cr,uid,ids,values,context=context)
        return True
        
    def process_close(self, cr, uid, ids, context=None):    
        _logger.info("Close Transaction for ID : " + str(ids))    
        #Post Calculation
        self._post_calculation(cr, uid, ids, context)            
        #Send Notification Email
        trans_id = ids[0] 
        trans = self._get_trans(cr, uid, trans_id, context=context)            
        rdm_config = self.pool.get('rdm.config').get_config(cr, uid, context=context)        
        if rdm_config.enable_email and trans.customer_id.receive_email:            
            self.send_mail_to_customer(cr, uid, [trans_id], context)                        
        return True

    def _update_print_status(self, cr, uid, ids, context=None):
        _logger.info("Start Update Print Status for ID : " + str(ids))
        values = {}
        values.update({'bypass':True})
        values.update({'method':'_update_print_status'})
        values.update({'printed':True})
        self.write(cr, uid, ids, values, context=context)
        _logger.info("End Update Print Status")
                
    def print_receipt(self, cr, uid, ids, context=None):
        _logger.info("Print Receipt for ID : " + str(ids))
        self._update_print_status(cr, uid, ids, context)        
        id = ids[0]   
        rdm_config = self.pool.get('rdm.config').get_config(cr, uid, context=context)
        serverUrl = 'http://' + reportserver + ':' + reportserverport +'/jasperserver'
        j_username = 'rdm_operator'
        j_password = 'rdm123'
        ParentFolderUri = '/rdm'
        reportUnit = '/rdm/trans_receipt'
        url = serverUrl + '/flow.html?_flowId=viewReportFlow&standAlone=true&_flowId=viewReportFlow&ParentFolderUri=' + ParentFolderUri + '&reportUnit=' + reportUnit + '&ID=' +  str(id) + '&decorate=no&j_username=' + j_username + '&j_password=' + j_password + '&output=pdf'
        return {
            'type':'ir.actions.act_url',
            'url': url,
            'nodestroy': True,
            'target': 'new' 
        }        
            
    def re_print(self, cr, uid, ids, context=None):
        _logger.info("Re-Print Receipt for ID : " + str(ids))
        return True
    
    def trans_reset(self, cr, uid, ids, context=None):
        _logger.info("Start Trans Reset for ID : " + str(ids))
        values = {}
        values.update({'bypass':True})
        values.update({'method':'trans_reset'})
        values.update({'state':'open'})
        self.write(cr, uid, ids, values, context=context)
        _logger.info("End Trans Reset")
        return True

    def trans_req_delete(self, cr, uid, ids, context=None):
        _logger.info("Start Trans Req Delete")        
        values = {}
        values.update({'bypass':True})
        values.update({'method':'trans_req_delete'})
        values.update({'state':'req_delete'})
        self.write(cr, uid, ids, values, context=context)
        _logger.info("End Trans Req Delete")
        return True
    
    def process_req_delete(self, cr, uid, ids, context=None):
        #self.write(cr,uid,ids,{'reg_delete':'done'},context=context)
        trans_id = ids[0]
        trans = self._get_trans(cr, uid, trans_id, context)
        rdm_config = self.pool.get('rdm.config').get_config(cr, uid, context=context)
        rdm_trans_config = self.pool.get('rdm.trans.config').get_config(cr, uid, context=context)
        if rdm_trans_config.trans_delete_allowed == True:
            values = {}
            values.update({'bypass':True})
            values.update({'method': 'trans_req_delete'})
            values.update({'state': 'req_delete'})
            self.write(cr, uid, ids, values, context=context)
            trans_detail_ids = trans.trans_detail_ids
                        
            for trans_detail in trans_detail_ids:
                self.pool.get('rdm.trans.detail').write(cr, uid, trans_detail.id, {'state':'req_delete'})            
                
            customer_coupon_ids = self.pool.get('rdm.customer.coupon').search(cr, uid, [('trans_id','=',trans_id)],context=context)
            self.pool.get('rdm.customer.coupon').write(cr, uid, customer_coupon_ids, {'state':'req_delete'})
            customer_point_ids = self.pool.get('rdm.customer.point').search(cr, uid, [('trans_id','=',trans_id)],context=context)
            self.pool.get('rdm.customer.point').write(cr, uid, customer_point_ids, {'state':'req_delete'})
            #Send Email to Approver
            email_data = {}
            email_data.update({'email_from':'info@taman-anggrek-mall.com'})
            approver_id = rdm_trans_config.trans_delete_approver
            approver = self.pool.get('hr.employee').browse(cr, uid, approver_id, context=context)
            email_data.update({'email_to':approver.work_email})
            subject = 'Request for Delete Transaction'
            email_data.update({'subject':subject})
            href =' http://' + rdm_config.rdm_server + ':8069/#id=' + str(trans_id) + '&view_type=form&model=rdm.trans&menu_id=131&action=114'
            msg = '<br/>'.join([
                    'Dear ' + approver.name,
                    '',
                    '',
                    'Please review this Delete Transaction Request',
                    '<a href="">Click here</a>'
                    '',
                    '',
                    'Regards',
                    '',
                    '',
                    'Redemption and Point Management System'
                ])
            email_data.update({'body_html': msg})
            self._send_email_notification(cr, uid, email_data, context)
            return True
        else:
            raise osv.except_osv(('Warning'), ('Request for delete not allowed!'))
    
    def trans_del_approve(self, cr, uid, ids, context=None):
        values = {}
        values.update({'bypass':True})
        values.update({'method': 'trans_del_approve'})
        values.update({'state': 'delete'})
        self.write(cr, uid, ids, values, context=context)
        return True
        
    def process_del_approve(self, cr, uid, ids, context=None):
        
        trans_id = ids[0]
        trans = self._get_trans(cr, uid, trans_id, context)            
        rdm_config = self.pool.get('rdm.config').get_config(cr, uid, context=context)
        rdm_trans_config = self.pool.get('rdm.trans.config').get_config(cr, uid, context=context)
        approver = self.pool.get('hr.employee').browse(cr, uid, [rdm_trans_config.trans_delete_approver], context=context)[0]
        if approver.user_id.id == uid:            
            trans_detail_ids = trans.trans_detail_ids
            for trans_detail in trans_detail_ids:
                self.pool.get('rdm.trans.detail').write(cr, uid, trans_detail.id, {'state':'delete'})
                
            customer_coupon_ids = self.pool.get('rdm.customer.coupon').search(cr, uid, [('trans_id','=',trans_id)],context=context)
            self.pool.get('rdm.customer.coupon').write(cr, uid, customer_coupon_ids, {'state':'delete'})
            customer_point_ids = self.pool.get('rdm.customer.point').search(cr, uid, [('trans_id','=',trans_id)],context=context)
            self.pool.get('rdm.customer.point').write(cr, uid, customer_point_ids, {'state':'delete'})            
        else:
            raise osv.except_osv(('Warning'), ('Approve Process not allowed!')) 

        return True
    
    def trans_del_reject(self, cr, uid, ids, context=None):
        values = {}
        values.update({'bypass':True})
        values.update({'method': 'trans_del_reject'})
        values.update({'state': 'done'})
        return self.write(cr, uid, ids, values, context=context)
                                
    def process_del_reject(self, cr, uid, ids, context=None):                    
                    
        trans_id = ids[0]
        trans = self._get_trans(cr, uid, trans_id, context)        
            
        rdm_config = self.pool.get('rdm.config').get_config(cr, uid, context=context)
        rdm_trans_config = self.pool.get('rdm.trans.config').get_config(cr, uid, context=context)
        approver = self.pool.get('hr.employee').browse(cr, uid, [rdm_trans_config.trans_delete_approver], context=context)[0]
        if approver.user_id.id == uid:
                        
            trans_detail_ids = trans.trans_detail_ids
            for trans_detail in trans_detail_ids:
                self.pool.get('rdm.trans.detail').write(cr, uid, trans_detail.id, {'state':'done'})
            
            customer_coupon_ids = self.pool.get('rdm.customer.coupon').search(cr, uid, [('trans_id','=',trans_id)],context=context)            
            self.pool.get('rdm.customer.coupon').write(cr, uid, customer_coupon_ids, {'state':'active'})
            
            customer_point_ids = self.pool.get('rdm.customer.point').search(cr, uid, [('trans_id','=',trans_id)],context=context)
            self.pool.get('rdm.customer.point').write(cr, uid, customer_point_ids, {'state':'active'})            
        else:
            raise osv.except_osv(('Warning'), ('Reject Process not allowed!')) 
        
        return True
    
    def _get_active_schemas(self, cr, uid, context=None):          
        _logger.info("Start Get Active Schemas")
        schemas_type = None            
        if context is None:
            context={}
        if context.get('default_type'):            
            schemas_type = context['default_type']                                
        schemas_id = None
        ids = None        
        if schemas_type == 'promo':     
            _logger.info("Type is Promo")       
            ids = self.pool.get('rdm.schemas').search(cr, uid, [('type','=','promo'),('state','=','open'),], context=context)
        if schemas_type == 'point':   
            _logger.info("Type is Point")
            ids = self.pool.get('rdm.schemas').search(cr, uid, [('type','=','point'),('state','=','open'),], context=context)        
        if ids :
            _logger.info("Active Promo Found")
            schemas_id = ids[0]                    
        else:                
            _logger.info("Active Promo not Found")
        _logger.info("End Get Active Promo")    
        return schemas_id       
                    
    def _get_trans(self, cr, uid, trans_id , context=None):
        return self.browse(cr, uid, trans_id, context=context);
            
    def _get_trans_schemas(self, cr, uid, ids, context=None):
        trans_id = ids[0]
        return self.pool.get('rdm.trans.schemas').browse(cr, uid, trans_id, context=context);
                
    def _get_trans_detail(self, cr, uid, trans_id, context=None):
        return self.pool.get('rdm.trans.detail').browse(cr, uid, trans_id, context=context)
                          
    def _get_schemas_rules(self, cr, uid, schemas_id, context=None):
        ids = self.pool.get('rdm.schemas.rules').search(cr, uid, [('schemas_id','=',schemas_id)], context=context);
        return self.pool.get('rdm.schemas.rules').browse(cr, uid, ids, context=context)
    
    def _get_customer_filters(self, cr, uid, ids, trans_schemas_id, context=None):
        trans_id = ids[0]                
        segment_status = False
        segment_message = "Segment not Allowed"
        gender_status = False
        gender_message = "Gender not Allowed"
        religion_status = False
        religion_message = "Religion not Allowed"
        ethnic_status = False
        ethnic_message = "Ethnic not Allowed"       
        marital_status = False
        marital_message = "Marital not Allowed"
        interest_status = False
        interest_message = "Interest not Allowed"
        cardtype_status = False
        cardtype_message= "Card Type not Allowed"
        message = ""
       
        trans = self._get_trans(cr, uid, trans_id, context=context)
        trans_schemas = self._get_trans_schemas(cr, uid, [trans_schemas_id], context=context)
        schemas = trans_schemas.schemas_id        
        customer = trans.customer_id
        
        #Filter Segment        
        _logger.info("Start Segment Filter")        
        if schemas.segment_ids:            
            for schemas_segment_id in schemas.segment_ids:                
                customer_age = datetime.date.today() - customer.birth_date
                if customer_age >= schemas_segment_id.start_age and customer_age <= schemas_segment_id.end_age:
                    segment_message = "Segment Allowed"
                    segment_status = True                    
        else:            
            segment_message = "Segment Allowed"
            segment_status = True         
        _logger.info("End Segment Filter")
        
        #Filter Gender    
        _logger.info("Start Gender Filter")                    
        if schemas.gender_ids:
            for schemas_gender_id in schemas.gender_ids:
                if schemas_gender_id.gender_id.id == customer.gender.id:
                    gender_message = "Gender Allowed"
                    gender_status = True        
        else:
            gender_message = "Gender Allowed"
            gender_status = True        
        _logger.info("End Gender Filter")
        
        #Filter Religion 
        _logger.info("Start Religion Filter")                    
        if schemas.religion_ids:
            for schemas_religion_id in schemas.religion_ids:
                if schemas_religion_id.religion_id.id == customer.religion.id:
                    religion_message = "Religion Allowed"                                                
                    religion_status = True                
        else:
            religion_message = "Religion Allowed"                                                
            religion_status = True
        _logger.info("End Religion Filter")
                            
        #Filter Ethnic
        _logger.info("Start Ethnic Filter")                                
        if schemas.ethnic_ids:
            for schemas_ethnic_id in schemas.ethnic_ids:
                if schemas_ethnic_id.ethnic_id.id == customer.ethnic.id:
                    ethnic_message = "Ethnic Allowed"                                                
                    ethnic_status = True
        else:
            ethnic_message = "Ethnic Allowed"                                                
            ethnic_status = True
        _logger.info("End Ethnic Filter")
                            
        #Filter Marital
        _logger.info("Start Marital Filter")                    
        if schemas.marital_ids:
            for schemas_marital_id in schemas.marital_ids:
                if schemas_marital_id.marital_id.id == customer.marital.id:
                    marital_message = "Marital Allowed"                                                
                    marital_status = True
        else:
            marital_message = "Marital Allowed"                                                
            marital_status = True
        _logger.info("End Marital Filter")                    
            
        #Filter Interest  
        _logger.info("Start Interest Filter")                    
        if schemas.interest_ids:
            for schemas_interest_id in schemas.interest_ids:
                if schemas_interest_id.interest_id.id == customer.interest.id:
                    interest_message = "Interest Allowed"                                                
                    interest_status = True                    
        else: 
            interest_message = "Interest Allowed"                                                
            interest_status = True                
        _logger.info("End Interest Filter")                    
        
        #Filter AYC Card Type
        _logger.info("Start AYC Card Type Filter")                    
        if schemas.card_type_ids:
            for schemas_card_type_id in schemas.card_type_ids:
                if schemas_card_type_id.card_type_id.id == customer.card_type.id:
                    cardtype_message = "Card Type Allowed"                                                
                    cardtype_status = True                
        else:
            cardtype_message = "Card Type Allowed"                                                
            cardtype_status = True     
        
        _logger.info("End AYC Card Type Filter")                    
            
        status = segment_status and gender_status and religion_status and ethnic_status and marital_status and interest_status and cardtype_status
        message = segment_message + "\n" + gender_message + "\n" + religion_message + "\n" + ethnic_message + "\n" + marital_message + "\n" + interest_message + "\n" + cardtype_message
        datas = {}
        
        if status == True:
            datas.update({'trans_filter':True})                                    
        
        datas.update({'remark': message})            
        self.pool.get('rdm.trans.schemas').write(cr, uid, [trans_schemas_id], datas, context=context)                    
        return None    
    
    def _get_tenant_filters(self, cr, uid, schemas_id, tenant, context=None):
        _logger.info('Start Tenant Filter')
        tenant_status = True
        tenant_category_status = True
        ayc_participant_status = True
        
        message = "Error tenant " + str(tenant.id) + " filter"        
        schemas_tenant_ids = schemas_id.tenant_ids
                    
        tenant_list = {}
        for schemas_tenant_id in schemas_tenant_ids:
            tenant_id = schemas_tenant_id.tenant_id
            tenant_list.update({tenant_id.id:tenant_id.name})
                        
        schemas_tenant_category_ids = schemas_id.tenant_category_ids        
        tenant_category_list = {}
        for schemas_tenant_category_id in schemas_tenant_category_ids:
            tenant_category_id = schemas_tenant_category_id.tenant_category_id
            tenant_category_list.update({tenant_category_id.id:tenant_category_id.name})
            
        schemas_ayc_participant_ids = schemas_id.ayc_participant_ids
        ayc_participant_list = {}
        
        for schemas_ayc_participant_id in schemas_ayc_participant_ids:
            ayc_participant_id = schemas_ayc_participant_id.participant_id
            ayc_participant_list.update({ayc_participant_id:ayc_participant_id}) 
        
        
        if tenant_list:
            if tenant.id in tenant_list.keys():
                tenant_status = True
            else:
                tenant_status = False           
        elif tenant_category_list:
            if tenant.category.id in tenant_category_list.keys():                
                tenant_category_status = True
            else:
                tenant_category_status = False
        elif ayc_participant_list:
            if tenant.participant in ayc_participant_list.keys():
                ayc_participant_status = True
            else:
                ayc_participant_status = False
         
        status = tenant_status and tenant_category_status and ayc_participant_status
                                                           
        message = ''
        
        if tenant_status:
            message = message + "Tenant Status True|"
        else:
            message = message + "Tenant Status False|"

        if tenant_category_status:
            message = message + "Tenant Category Status True|"
        else:
            message = message + "Tenant Category Status False|"
            
        if ayc_participant_status:
            message = message + "Ayc Participant Status True|"
        else:
            message = message + "Ayc Participant Status False|"

        _logger.info('End Tenant Filter')                            
        return status, message    
    
    def _set_trans_id(self, cr, uid, ids, context=None):
        _logger.info('Start Set Trans ID Filter')
        trans_id = ids[0]
        trans = self._get_trans(cr, uid, trans_id, context)
        if trans.type == 'promo':
            trans_seq_id = self.pool.get('ir.sequence').get(cr, uid, 'rdm.trans.redemption.sequence'),
        if trans.type == 'point':
            trans_seq_id = self.pool.get('ir.sequence').get(cr, uid, 'rdm.trans.point.sequence'),            
        trans_data = {}
        trans_data.update({'trans_id':trans_seq_id[0]})        
        super(rdm_trans,self).write(cr, uid, [trans_id], trans_data, context=context)
        _logger.info('End Set Trans ID Filter')
        
    def _get_total_amount(self, cr, uid, ids, context=None):
        _logger.info('Start Get Total Filter')
        trans_id = ids[0]        
        trans = self._get_trans(cr, uid, trans_id, context)
        
        total_amount = 0
        total_item = 0        
        for trans_detail in trans.trans_detail_ids:
            total_amount = total_amount + trans_detail.total_amount
            total_item = total_item + trans_detail.total_item

        trans_data = {}
        trans_data.update({'total_amount':total_amount})
        trans_data.update({'total_item':total_item})
        super(rdm_trans,self).write(cr, uid, [trans_id], trans_data, context=context)
        _logger.info('End Get Total Filter')

    def _get_valid_total(self, cr, uid, ids, trans_schemas_id, context=None):
        _logger.info('Start Get Valid Total Filter')        
        trans_id = ids[0]
        trans = self._get_trans(cr, uid, trans_id, context)
        trans_schemas_ids = trans.trans_schemas_ids
        for trans_schemas_id in trans_schemas_ids:            
            schemas_id = trans_schemas_id.schemas_id
            valid_amount = 0                            
            for trans_detail in trans.trans_detail_ids:
                tenant_id = trans_detail.tenant_id
                status, message = self._get_tenant_filters(cr, uid, schemas_id, tenant_id, context=context)
                _logger.info(message)
                if status:        
                    valid_amount = valid_amount + trans_detail.total_amount
                
            trans_schemas_data = {}
            if trans_schemas_id.trans_filter == True:
                trans_schemas_data.update({'valid_amount':valid_amount})
            else:
                trans_schemas_data.update({'valid_amount':valid_amount})        
                            
            self.pool.get('rdm.trans.schemas').write(cr, uid, [trans_schemas_id.id], trans_schemas_data, context=context)
            _logger.info('End Get Valid Total Filter')    
 
    
    def _calculate_add_coupon_and_point(self, cr, uid, trans_id, context=None):
        _logger.info('Start Calculate Add Coupon and Point')
        
        trans = self._get_trans(cr, uid, trans_id, context)        
        trans_schemas_ids = trans.trans_schemas_ids
        trans_detail_ids =  trans.trans_detail_ids
        customer_id = trans.customer_id                          
        
        for trans_schemas_id in trans_schemas_ids:
            coupon = 0
            point = 0
            
            schemas_id = trans_schemas_id.schemas_id               
            schemas_rules_ids = schemas_id.rules_ids
            max_spend_amount = schemas_id.max_spend_amount
            coupon_spend_amount = schemas_id.coupon_spend_amount
            point_spend_amount = schemas_id.point_spend_amount
            trans_detail_list = {}                    
                                
            for trans_detail_id in trans_detail_ids:       
                _logger.info('-- Calculate for Trans Detail id ' + str(trans_detail_id.id) +' --')
                ##current_day_spend_amount = self.transactions_total_amount(cr, uid, [trans.id], context)
                
                current_day_spend_amount = self.transactions_total_amount(cr, uid, trans_detail_id, schemas_id, customer_id, context)                                                 
                _logger.info('Current Day Spend Amount : ' +  str(current_day_spend_amount))                
                tenant = trans_detail_id.tenant_id                
                bank_id = trans_detail_id.bank_id
                bank_card_id = trans_detail_id.bank_card_id                
                payment_type = trans_detail_id.payment_type                
                
                #if max_spend_amount == -1:
                #    _logger.info('Unlimited Spend Amount')
                #    diff_spend_amount = trans_detail_id.total_amount
                #else:
                #    _logger.info('Limited Spend Amount')    
                #    diff_spend_amount  = max_spend_amount - current_day_spend_amount
                
                # _logger.info('Diff Spend Amount : ' + str(diff_spend_amount))
                
                #if diff_spend_amount <= 0:
                #    total_amount = 0          
                #else:                         
                #    if diff_spend_amount >= trans_detail_id.total_amount:
                #        total_amount = trans_detail_id.total_amount
                #    else:                                                                                    
                #        total_amount = diff_spend_amount
                
                total_amount = trans_detail_id.total_amount
                     
                if coupon_spend_amount == 0:
                    coupon = 0
                else:                    
                    coupon = total_amount / coupon_spend_amount
                
                if point_spend_amount == 0:
                    point = 0
                else:                    
                    point = total_amount / point_spend_amount
                
                rules_add_ditotal_coupon = 0
                rules_add_terbesar_coupon = 0
                rules_add_ditotal_point = 0
                rules_add_terbesar_point = 0
                
                rules_multiple_ditotal_coupon = 1                
                rules_multiple_ditotal_point = 1
                rules_multiple_terbesar_coupon = 1                
                rules_multiple_terbesar_point = 1
                                
                
                schemas_status, message = self._get_tenant_filters(cr, uid, schemas_id, tenant, context=context)
                
                if schemas_status:                    
                    for schemas_rules_id in schemas_rules_ids:                                                                                                                                        
                        rules = schemas_rules_id.rules_id
                        _logger.info("Check Rule : " + rules.name)
                        calculation = schemas_rules_id.schemas
                        _logger.info('Calculation : ' + str(calculation))                                      
                        apply_for = rules.apply_for
                        _logger.info('Apply For : ' + apply_for)                                        
                        operation = rules.operation
                        _logger.info('Operation : ' + operation)
                        quantity = rules.quantity
                        _logger.info('Quantity : ' + str(quantity))                        
                        rules_detail_ids = rules.rules_detail_ids
                        status = True                   
                        for rules_detail_id in rules_detail_ids:
                            rule_schema = rules_detail_id.rule_schema
                            rules_detail_operation = rules_detail_id.operation
                            #Get Rules Status and Return True if valid rules     
                            #Birthday
                            if rule_schema == 'birthday':
                                _logger.info('Start Birthday Schemas')
                                today = datetime.date.today().strftime("%Y-%m-%d")
                                today_day = datetime.date.today().day
                                today_month = datetime.date.today().month  
                                _logger.info('Today : ' + today)
                                
                                birthdate = datetime.datetime.strptime(customer_id.birth_date,'%Y-%m-%d')                                                    
                                birthdate_day = birthdate.day
                                birthdate_month = birthdate.month                                    
                                _logger.info('Birth Date : ' + customer_id.birth_date)
                                
                                if today_day == birthdate_day and today_month == birthdate_month :
                                    _logger.info('Rules Birthday Match')
                                    if rules_detail_operation == 'or':
                                        status = status or True
                                    if rules_detail_operation == 'and':
                                        status = status and True                                                                        
                                else: 
                                    if rules_detail_operation == 'or':
                                        status = status or False
                                    if rules_detail_operation == 'and':
                                        status = status and False    
                                    
                            
                            #Gender 
                            if rule_schema == 'gender':
                                _logger.info('Start Gender Schemas')
                                rule_gender_ids = rules_detail_id.gender_ids
                                gender_list = {}   
                                for rule_gender in rule_gender_ids:
                                    _logger.info('Filled Gender List')
                                    rule_gender_id  = rule_gender.gender_id.id
                                    rule_gender_name = rule_gender.gender_id.name
                                    gender_list.update({rule_gender_id:rule_gender_name})
                        
                                if customer_id.gender.id in gender_list.keys():
                                    _logger.info('Match Gender : ' + customer_id.gender.name)
                                    if rules_detail_operation == 'or':
                                        status = status or True
                                    if rules_detail_operation == 'and':
                                        status = status and True                                                        
                                else: 
                                    if rules_detail_operation == 'or':
                                        status = status or False
                                    if rules_detail_operation == 'and':
                                        status = status and False
                                                                    
                            #Day Schemas
                            if rule_schema == 'day':
                                _logger.info('Start Day Schemas')                
                                today = datetime.date.today().strftime("%Y-%m-%d")
                                day = rules_detail_id.day
                                if today == day :
                                    _logger.info('Match Day : ' + today)
                                    if rules_detail_operation == 'or':
                                        status = status or True
                                    if rules_detail_operation == 'and':
                                        status = status and True                                                        
                                else: 
                                    if rules_detail_operation == 'or':
                                        status = status or False
                                    if rules_detail_operation == 'and':
                                        status = status and False
                                        
    
                            #Day Name Schemas
                            if rule_schema == 'dayname':
                                _logger.info('Start Day Name Schemas')                        
                                weekday = datetime.datetime.today().weekday()                        
                                dayname = rules_detail_id.day_name
                                if weekday == 0:
                                    day = '01'
                                if weekday == 1:
                                    day = '02'
                                if weekday == 2:
                                    day = '03'
                                if weekday == 3:
                                    day = '04'
                                if weekday == 4:
                                    day = '05'
                                if weekday == 5:
                                    day = '06'
                                if weekday == 6:
                                    day = '07'
                                               
                                if dayname == day:
                                    _logger.info('Match Day Name : ' + day)
                                    if rules_detail_operation == 'or':
                                        status = status or True
                                    if rules_detail_operation == 'and':
                                        status = status and True                                                        
                                else: 
                                    if rules_detail_operation == 'or':
                                        status = status or False
                                    if rules_detail_operation == 'and':
                                        status = status and False
                                        
                                _logger.info('End Day Name Schemas')     
                                                                                                                                                                      
                            #Card Type
                            if rule_schema == 'cardtype':
                                card_type_rules = False
                                _logger.info('Start Card Type Schemas')
                                customer_card_type = customer_id.card_type
                                card_type_ids = rules.card_type_ids
                                for card_type in card_type_ids:
                                    if customer_card_type.id == card_type.id:
                                        card_type_rules = True
                                        
                                if card_type_rules == True:
                                    _logger.info('Match Card Type')
                                    if rules_detail_operation == 'or':
                                        status = status or True
                                    if rules_detail_operation == 'and':
                                        status = status and True                                                        
                                else: 
                                    if rules_detail_operation == 'or':
                                        status = status or False
                                    if rules_detail_operation == 'and':
                                        status = status and False
                                
                            #Age
                            if rule_schema == 'age':
                                _logger.info('Start Age Schemas')
                                customer_birthdate = datetime.datetime.strptime(customer_id.birth_date , '%Y-%m-%d')                        
                                customer_age_diff =  datetime.datetime.now() - customer_birthdate
                                customer_age = (customer_age_diff.days + customer_age_diff.seconds/86400)/365                                            
                                age_ids = rules_detail_id.age_ids
                                age_rules = False
                                for age_id in age_ids:
                                    if age_id.operator == 'eq':
                                        if customer_age == age_id.value1:
                                            age_rules = True
                                    if age_id.operator == 'ne':
                                        if customer_age != age_id.value1:
                                            age_rules = True                                    
                                    if age_id.operator == 'lt':
                                        if customer_age < age_id.value1:
                                            age_rules = True
                                    if age_id.operator == 'gt':
                                        if customer_age > age_id.value1:
                                            age_rules = True
                                    if age_id.operator == 'bw':
                                        if customer_age >= age_id.value1 and customer_age <= age_id.value2:
                                            age_rules = True
                                                                    
                                if age_rules == True:
                                    _logger.info('Match Age')
                                    if rules_detail_operation == 'or':
                                        status = status or True
                                    if rules_detail_operation == 'and':
                                        status = status and True                                                        
                                else: 
                                    if rules_detail_operation == 'or':
                                        status = status or False
                                    if rules_detail_operation == 'and':
                                        status = status and False                                                        
                                                            
                                _logger.info('End Age Schemas')
                            
                            #Spending
                            if rule_schema == 'spending':
                                _logger.info('Start Spending Amount Schemas')
                                spending_amount_ids = rules_detail_id.spending_amount_ids                            
                                spending_amount_rules = False
                                
                                for spending_amount_id in spending_amount_ids:
                                    if spending_amount_id.operator == 'eq':
                                        if trans_detail_id.total_amount == spending_amount_id.value1:
                                            spending_amount_rules = True
                                    if spending_amount_id.operator == 'ne':
                                        if trans_detail_id.total_amount != spending_amount_id.value1:
                                            spending_amount_rules = True                                    
                                    if spending_amount_id.operator== 'lt':
                                        if trans_detail_id.total_amount < spending_amount_id.value1:
                                            spending_amount_rules = True
                                    if spending_amount_id.operator == 'gt':
                                        if trans_detail_id.total_amount > spending_amount_id.value1:
                                            spending_amount_rules = True
                                    if spending_amount_id.operator == 'bw':
                                        if trans_detail_id.total_amount >= spending_amount_id.value1 and trans_detail_id.total_amount <= spending_amount_id.value2:
                                            spending_amount_rules = True
                                                                    
                                if spending_amount_rules == True:
                                    _logger.info('Match Spending Amount')
                                    if rules_detail_operation == 'or':
                                        status = status or True
                                    if rules_detail_operation == 'and':
                                        status = status and True                                                        
                                else: 
                                    if rules_detail_operation == 'or':
                                        status = status or False
                                    if rules_detail_operation == 'and':
                                        status = status and False                                                        
                                                            
                                _logger.info('End Spending Schemas')
                                                        
                            #Participant
                            if rule_schema == 'participant':
                                participant_ids  = rules_detail_id.participant_ids
                                participant_list = {}
                                for participant_id in participant_ids:
                                    participant = participant_id.participant_id
                                    participant_list.update({participant:participant})
                                
                                participant_rules = False
                                if tenant.participant in participant_list.keys():
                                    participant_rules = True
                                                                
                                if participant_rules == True:
                                    _logger.info('Match Participant')
                                    if rules_detail_operation == 'or':
                                        status = status or True
                                    if rules_detail_operation == 'and':
                                        status = status and True                                                        
                                else: 
                                    if rules_detail_operation == 'or':
                                        status = status or False
                                    if rules_detail_operation == 'and':
                                        status = status and False                                                        
                                
                            #Tenant Type     
                            if rule_schema == 'tenanttype':
                                _logger.info('Start Tenant Type Schemas')   
                                total_amount = 0                                                
                                rules_tenant_category_ids = rules_detail_id.tenant_category_ids
                                
                                tenant_category_list = {}                            
                                for rules_tenant_category_id in rules_tenant_category_ids:
                                    tenant_category = rules_tenant_category_id.tenant_category_id
                                    tenant_category_list.update({tenant_category.id:tenant_category.name})
                                                                    
                                tenanttype_rules = False                            
                                if tenant.category.id in tenant_category_list.keys():
                                    tenanttype_rules = True                                           
                                                                                                            
                                if tenanttype_rules:                        
                                    _logger.info('Match Tenant Type')
                                    if rules_detail_operation == 'or':
                                        status = status or True
                                    if rules_detail_operation == 'and':
                                        status = status and True                                                        
                                else: 
                                    if rules_detail_operation == 'or':
                                        status = status or False
                                    if rules_detail_operation == 'and':
                                        status = status and False                                             
                                _logger.info('End Tenant Type Schemas')                            
                                                                                                            
                            #Tenant     
                            if rule_schema == 'tenant':
                                _logger.info('Start Tenant Schemas')             
                                
                                total_amount = 0          
                                rules_tenant_ids = rules_detail_id.tenant_ids 
                                tenant_list = {}
                                for rules_tenant_id in rules_tenant_ids:
                                    tenant_id = rules_tenant_id.tenant_id                            
                                    tenant_list.update({tenant_id.id:tenant_id.name})                                       
                                                                                                 
                                tenant_rules = False                                                                                                                        
                                if tenant.id in tenant_list.keys():
                                    tenant_rules = True
                                                                                                                                                    
                                if tenant_rules:                        
                                    _logger.info('Match Tenant')
                                    if rules_detail_operation == 'or':
                                        status = status or True
                                    if rules_detail_operation == 'and':
                                        status = status and True                                                        
                                else: 
                                    if rules_detail_operation == 'or':
                                        status = status or False
                                    if rules_detail_operation == 'and':
                                        status = status and False     
                                                                        
                                _logger.info('End Tenant Schemas')
                                                           
                            #Bank     
                            if rule_schema == 'bank':
                                _logger.info('Start Bank Schemas')         
                                
                                rules_bank_ids = rules_detail_id.bank_ids
                                bank_card_list = {}
                                for rules_bank in rules_bank_ids:
                                    bank  = rules_bank.bank_id
                                    bank_card_list.update({bank.id:bank.name})
                                                                        
                                bank_rules = False                                                
                                if payment_type == 'creditcard' or payment_type == 'debit':                                    
                                        if bank_id.id in bank_card_list.keys():
                                            bank_rules = True                                            
                                                                                                                    
                                if bank_rules:                        
                                    _logger.info('Match Bank')
                                    if rules_detail_operation == 'or':
                                        status = status or True
                                    if rules_detail_operation == 'and':
                                        status = status and True                                                        
                                else: 
                                    if rules_detail_operation == 'or':
                                        status = status or False
                                    if rules_detail_operation == 'and':
                                        status = status and False                                                                              
                            
                            #Bank Card     
                            if rule_schema == 'bankcard':
                                _logger.info('Start Bank Card Schemas')    
                                total_amount = 0
                                rules_bank_card_ids = rules_detail_id.bank_card_ids
                                bank_card_list = {}
                                for rules_bank_card in rules_bank_card_ids:
                                    bank_card_id = rules_bank_card.bank_card_id
                                    bank_card_list.update({bank_card_id.id:bank_card_id.name})
                                        
                                trans_detail_ids = trans.trans_detail_ids
                                bank_card_rules = True                                                
                                if payment_type == 'creditcard' or payment_type == 'debit':                                    
                                    if bank_card_id.id in bank_card_list.keys():
                                        bank_card_rules = False
                                            
                                if bank_card_rules:                        
                                    _logger.info('Match Bank Card')
                                    if rules_detail_operation == 'or':
                                        status = status or True
                                    if rules_detail_operation == 'and':
                                        status = status and True                                                        
                                else: 
                                    if rules_detail_operation == 'or':
                                        status = status or False
                                    if rules_detail_operation == 'and':
                                        status = status and False    
                            
                            #Cash     
                            if rule_schema == 'cash':
                                _logger.info('Start Cash Schemas')                                         
                                rules_cash_bank_ids = rules_detail_id.cash_ids
                                cash_bank_list = {}
                                for rules_cash_bank in rules_cash_bank_ids:
                                    cash_bank  = rules_cash_bank.bank_id
                                    cash_bank_list.update({cash_bank.id:cash_bank.name})
                                                                        
                                cash_rules = True
                                if payment_type == 'creditcard' or payment_type == 'debit':
                                    if bank_id.id in cash_bank_list.keys():
                                        _logger.info('Card Detected')
                                        cash_rules = False           
                                    else:
                                        _logger.info('Card Not Detected') 
                                                                                                                    
                                if cash_rules:                        
                                    _logger.info('Match Cash')
                                    if rules_detail_operation == 'or':
                                        status = status or True
                                    if rules_detail_operation == 'and':
                                        status = status and True                                                        
                                else: 
                                    if rules_detail_operation == 'or':
                                        status = status or False
                                    if rules_detail_operation == 'and':
                                        status = status and False        
                                                           
                        if status == True:         
                            _logger.info('Status True')               
                            if operation == 'add':
                                if calculation == 'ditotal':
                                    if apply_for == '1':
                                        rules_add_ditotal_coupon = rules_add_ditotal_coupon + Decimal(quantity)
                                                                        
                                    if apply_for == '2':
                                        rules_add_ditotal_point = rules_add_ditotal_point + Decimal(quantity)                                                                    
                                                                                                                  
                                if calculation == 'terbesar':
                                    if apply_for == '1':                                
                                        if rules_add_terbesar_coupon < Decimal(quantity):
                                            rules_add_terbesar_coupon = Decimal(quantity)
                                    
                                    if  apply_for == '2':
                                        if rules_add_terbesar_point < Decimal(quantity):
                                            rules_add_terbesar_point = Decimal(quantity)
                                      
                            if operation == 'multiple':
                                if calculation == 'ditotal':
                                    if apply_for == '1':                                                                     
                                        rules_multiple_ditotal_coupon = rules_multiple_ditotal_coupon * Decimal(quantity)
                                        
                                    if apply_for == '2':
                                        rules_multiple_ditotal_point = rules_multiple_ditotal_point * Decimal(quantity)
                                            
                                if calculation == 'terbesar':
                                    if apply_for == '1':
                                        if rules_multiple_terbesar_coupon < Decimal(quantity):
                                            rules_multiple_terbesar_coupon = Decimal(quantity)
                                    
                                    if apply_for == '2':
                                        if Decimal(quantity) == 0:
                                            rules_multiple_terbesar_point = 0
                                            
                                        if rules_multiple_terbesar_point < Decimal(quantity):
                                            rules_multiple_terbesar_point = Decimal(quantity)
                                    
                        else:
                            _logger.info('Status False')
                                                                       
                else:
                    rules_multiple_ditotal_coupon = 0
                    rules_add_ditotal_coupon = 0
                    rules_add_terbesar_coupon = 0
                    rules_multiple_ditotal_point = 0
                    rules_add_ditotal_point = 0                                                                                                                                                                                                                                                            
                
                _logger.info('Coupon : ' + str(coupon))
                _logger.info('Mutliple Ditotal Coupon : ' + str(rules_multiple_ditotal_coupon))
                _logger.info('Add Ditotal Coupon : ' + str(rules_add_ditotal_coupon))
                _logger.info('Add Terbesar Coupon : ' + str(rules_add_terbesar_coupon))
                                                               
                _logger.info('Point : ' + str(point))
                _logger.info('Mutliple Ditotal Point : ' + str(rules_multiple_ditotal_point))
                _logger.info('Add Ditotal Point : ' + str(rules_add_ditotal_point))
                _logger.info('Add Terbesar Point : ' + str(rules_add_terbesar_point))
                                                               
                                                                                                                                               
                if coupon == None:
                    coupon = 1
                    result_coupon = (Decimal(coupon) * rules_multiple_ditotal_coupon * rules_multiple_terbesar_coupon) + (rules_add_ditotal_coupon + rules_add_terbesar_coupon)
                else:
                    result_coupon = (Decimal(coupon) * rules_multiple_ditotal_coupon * rules_multiple_terbesar_coupon) + (rules_add_ditotal_coupon + rules_add_terbesar_coupon)
                    
                if point == None:
                    point = 1
                    result_point = (Decimal(point) * rules_multiple_ditotal_point * rules_multiple_terbesar_point) + (rules_add_ditotal_point + rules_add_terbesar_point)
                else:                    
                    result_point = (Decimal(point) * rules_multiple_ditotal_point * rules_multiple_terbesar_point) + (rules_add_ditotal_point + rules_add_terbesar_point)
                
                _logger.info('Total Coupon : ' + str(result_coupon))                                                               
                _logger.info('Total Point : ' + str(result_point))           
                
                trans_detail_coupon_data = {}
                trans_detail_coupon_data.update({'trans_id': trans.id})
                trans_detail_coupon_data.update({'trans_detail_id': trans_detail_id.id})
                trans_detail_coupon_data.update({'trans_schemas_id': trans_schemas_id.id})
                trans_detail_coupon_data.update({'basic': coupon})
                trans_detail_coupon_data.update({'coupon': result_coupon})
                trans_detail_coupon_data.update({'multiple_ditotal': rules_multiple_ditotal_coupon})
                trans_detail_coupon_data.update({'multiple_terbesar': rules_multiple_terbesar_coupon})                
                trans_detail_coupon_data.update({'add_ditotal': rules_add_ditotal_coupon})
                trans_detail_coupon_data.update({'add_terbesar': rules_add_terbesar_coupon})                                            
                self.pool.get('rdm.trans.detail.coupon').create(cr, uid, trans_detail_coupon_data, context=context)                    
                
                        
                trans_detail_point_data = {}
                trans_detail_point_data.update({'trans_id': trans.id})
                trans_detail_point_data.update({'trans_detail_id': trans_detail_id.id})
                trans_detail_point_data.update({'trans_schemas_id': trans_schemas_id.id})
                trans_detail_point_data.update({'basic': point})
                trans_detail_point_data.update({'point': result_point})
                trans_detail_point_data.update({'multiple_ditotal': rules_multiple_ditotal_point})
                trans_detail_point_data.update({'multiple_terbesar': rules_multiple_terbesar_point})
                trans_detail_point_data.update({'add_ditotal': rules_add_ditotal_point})
                trans_detail_point_data.update({'add_terbesar': rules_add_terbesar_point})                                          
                self.pool.get('rdm.trans.detail.point').create(cr, uid, trans_detail_point_data, context=context)
                
                #Update Redemption Trans Detail Status For Already Calculated         
                #self.pool.get('rdm.trans.detail').trans_close(cr, uid, [trans_detail_id.id], context=context)           
                _logger.info('Change Transaction Detail State to Done')
                                
        _logger.info('End Calculate Add Coupon and Point')
                    
    def _calculate_trans_priority_per_schemas(self, cr , uid, trans_id, context=None):
        _logger.info('Start Calculate Valid Trans Per Schemas')
        trans = self._get_trans(cr, uid, trans_id, context)            
        trans_schemas_ids = trans.trans_schemas_ids        
        for trans_schemas_id in trans_schemas_ids:            
            number_of_detail = len(trans_schemas_id.trans_detail_coupon_ids)
            priority=0                
            for num in range(0,number_of_detail):                            
                args = [('trans_schemas_id','=',trans_schemas_id.id),('state','=','open')]
                trans_detail_coupon_ids = self.pool.get('rdm.trans.detail.coupon').search(cr, uid, args, context=context)
                trans_detail_coupons = self.pool.get('rdm.trans.detail.coupon').browse(cr, uid, trans_detail_coupon_ids, context=context)
                min_basic = 0
                start = True
                for trans_detail_coupon in trans_detail_coupons:
                    if start:                                    
                        min_basic = trans_detail_coupon.basic
                        start = False
                    else: 
                        if min_basic > trans_detail_coupon.basic:
                            min_basic = trans_detail_coupon.basic
                            
                max_coupon = 0
                start = True                
                for trans_detail_coupon in trans_detail_coupons:
                    if start:
                        start = False
                        min_basic_coupon = min_basic * trans_detail_coupon.multiple_ditotal * trans_detail_coupon.multiple_terbesar + (trans_detail_coupon.add_ditotal + trans_detail_coupon.add_terbesar)
                        trans_detail_coupon_id = trans_detail_coupon.id
                        max_coupon = min_basic_coupon
                    else:
                        min_basic_coupon = min_basic * trans_detail_coupon.multiple_ditotal * trans_detail_coupon.multiple_terbesar + (trans_detail_coupon.add_ditotal + trans_detail_coupon.add_terbesar)
                        if max_coupon < min_basic_coupon:
                            trans_detail_coupon_id = trans_detail_coupon.id
                            max_coupon = min_basic_coupon
                                                            
                priority = priority + 1
                trans_data = {}
                trans_data.update({'priority':priority})
                trans_data.update({'state':'done'})                                
                self.pool.get('rdm.trans.detail.coupon').write(cr, uid, [trans_detail_coupon_id], trans_data, context=context)

            number_of_detail = len(trans_schemas_id.trans_detail_point_ids)
            priority=0                
            for num in range(0,number_of_detail):                            
                args = [('trans_schemas_id','=',trans_schemas_id.id),('state','=','open')]
                trans_detail_point_ids = self.pool.get('rdm.trans.detail.point').search(cr, uid, args, context=context)
                trans_detail_points = self.pool.get('rdm.trans.detail.point').browse(cr, uid, trans_detail_point_ids, context=context)
                min_basic = 0
                start = True
                for trans_detail_point in trans_detail_points:
                    if start:                                    
                        min_basic = trans_detail_point.basic
                        start = False
                    else: 
                        if min_basic > trans_detail_point.basic:
                            min_basic = trans_detail_point.basic
                            
                max_point = 0
                start = True                
                for trans_detail_point in trans_detail_points:
                    if start:
                        start = False
                        min_basic_point = min_basic * trans_detail_point.multiple_ditotal * trans_detail_point.multiple_terbesar + (trans_detail_point.add_ditotal + trans_detail_point.add_terbesar)
                        trans_detail_point_id = trans_detail_point.id
                        max_point = min_basic_point
                    else:
                        min_basic_point = min_basic * trans_detail_point.multiple_ditotal * trans_detail_point.multiple_terbesar + (trans_detail_point.add_ditotal + trans_detail_point.add_terbesar)
                        if max_point < min_basic_point:
                            trans_detail_point_id = trans_detail_point.id
                            max_point = min_basic_point
                                                            
                priority = priority + 1
                
                trans_data = {}
                trans_data.update({'priority':priority})
                trans_data.update({'state':'done'})                                                            
                self.pool.get('rdm.trans.detail.point').write(cr, uid, [trans_detail_point_id], trans_data, context=context)
                                                                                                                
        _logger.info('End Calculate Valid Trans Per Schemas')
    
    
    def _calculate_valid_coupon_and_point(self, cr , uid, trans_id, context=None):
        _logger.info('Start Calculate Valid Coupon and Point')
        trans = self._get_trans(cr, uid, trans_id, context=context)        
        trans_schemas_ids = trans.trans_schemas_ids
        
        for trans_schemas_id in trans_schemas_ids:
            schemas_id = trans_schemas_id.schemas_id 
            max_spend_amount = schemas_id.max_spend_amount
            coupon_spend_amount = schemas_id.coupon_spend_amount
            point_spend_amount = schemas_id.point_spend_amount
                                   
            customer_id = trans.customer_id            
            current_day_spend_amount = self.current_total_amount(cr, uid, customer_id, context)                                                                        
            _logger.info('Current Day Spend Amount : ' +  str(current_day_spend_amount))       
            
            args = [('trans_schemas_id','=',trans_schemas_id.id)]
            trans_detail_coupon_ids = self.pool.get('rdm.trans.detail.coupon').search(cr, uid, args, order="priority asc",context=context)
            trans_detail_coupons = self.pool.get('rdm.trans.detail.coupon').browse(cr, uid, trans_detail_coupon_ids, context=context)
            
            for trans_detail_coupon in trans_detail_coupons:                
                trans_detail_id = trans_detail_coupon.trans_detail_id
                
                rules_multiple_ditotal_coupon = trans_detail_coupon.multiple_ditotal
                rules_multiple_terbesar_coupon = trans_detail_coupon.multiple_terbesar
                rules_add_ditotal_coupon = trans_detail_coupon.add_ditotal
                rules_add_terbesar_coupon = trans_detail_coupon.add_terbesar
                
                if max_spend_amount == -1:
                    _logger.info('Unlimited Spend Amount')
                    diff_spend_amount = trans_detail_id.total_amount
                else:
                    _logger.info('Limited Spend Amount')    
                    diff_spend_amount  = max_spend_amount - current_day_spend_amount            
                _logger.info('Diff Spend Amount : ' + str(diff_spend_amount))
                
                if diff_spend_amount <= 0:
                    total_amount = 0          
                else:                         
                    if diff_spend_amount >= trans_detail_id.total_amount:
                        total_amount = trans_detail_id.total_amount                        
                        current_day_spend_amount = current_day_spend_amount + total_amount
                        trans_data = {}
                        valid_coupon = trans_detail_coupon.coupon 
                        trans_data.update({'valid_coupon': valid_coupon})
                        self.pool.get('rdm.trans.detail.coupon').write(cr, uid, [trans_detail_coupon.id], trans_data, context=context)
                                                
                    else:                                                                                    
                        total_amount = diff_spend_amount                        
                        #Check Allow Generate Coupon       
                        if coupon_spend_amount != 0:      
                            valid_basic_coupon = total_amount / coupon_spend_amount
                            valid_coupon =  (valid_basic_coupon * rules_multiple_ditotal_coupon * rules_multiple_terbesar_coupon) + (rules_add_ditotal_coupon + rules_add_terbesar_coupon)
                        else:
                            valid_coupon = 0                            
                        trans_data = {}                        
                        trans_data.update({'valid_coupon': valid_coupon})
                        self.pool.get('rdm.trans.detail.coupon').write(cr, uid, [trans_detail_coupon.id], trans_data, context=context)
                
            current_day_spend_amount = self.current_total_amount(cr, uid, customer_id, context)    
            
            trans_detail_point_ids = self.pool.get('rdm.trans.detail.point').search(cr, uid, args, order="priority asc",context=context)
            trans_detail_points = self.pool.get('rdm.trans.detail.point').browse(cr, uid, trans_detail_point_ids, context=context)
            
            for trans_detail_point in trans_detail_points:                
                trans_detail_id = trans_detail_point.trans_detail_id
                
                rules_multiple_ditotal_point = trans_detail_point.multiple_ditotal
                rules_multiple_terbesar_point = trans_detail_point.multiple_terbesar
                rules_add_ditotal_point = trans_detail_point.add_ditotal
                rules_add_terbesar_point = trans_detail_point.add_terbesar
                
                if max_spend_amount == -1:
                    _logger.info('Unlimited Spend Amount')
                    diff_spend_amount = trans_detail_id.total_amount
                else:
                    _logger.info('Limited Spend Amount')    
                    diff_spend_amount  = max_spend_amount - current_day_spend_amount            
                _logger.info('Diff Spend Amount : ' + str(diff_spend_amount))
                
                if diff_spend_amount <= 0:
                    total_amount = 0          
                else:                         
                    if diff_spend_amount >= trans_detail_id.total_amount:
                        total_amount = trans_detail_id.total_amount                        
                        current_day_spend_amount = current_day_spend_amount + total_amount
                        trans_data = {}
                        valid_point = trans_detail_point.point 
                        trans_data.update({'valid_point': valid_point})
                        self.pool.get('rdm.trans.detail.point').write(cr, uid, [trans_detail_point.id], trans_data, context=context)
                                                
                    else:                                                                                    
                        total_amount = diff_spend_amount
                        #Check Allow Generate Point
                        if point_spend_amount != 0:                                                            
                            valid_basic_point = total_amount / point_spend_amount
                            valid_point =  (valid_basic_point * rules_multiple_ditotal_point * rules_multiple_terbesar_point) + (rules_add_ditotal_point + rules_add_terbesar_point)
                        else: 
                            valid_point = 0
                        trans_data = {}                        
                        trans_data.update({'valid_point': valid_point})
                        self.pool.get('rdm.trans.detail.point').write(cr, uid, [trans_detail_point.id], trans_data, context=context)
                                                                                                                                            
        _logger.info('End Calculate Valid Coupon and Point') 
    
    def _close_trans_detail(self, cr, uid, trans_id, context=None):
        _logger.info('Start Close Trans Detail')
        trans = self._get_trans(cr, uid, trans_id, context)
        trans_detail_ids = trans.trans_detail_ids
        
        for trans_detail_id in trans_detail_ids:
            trans_data = {}
            trans_data.update({'state':'done'})
            self.pool.get('rdm.trans.detail').write(cr, uid, [trans_detail_id.id], trans_data, context=context)
            
        _logger.info('End Close Trans Detail')    
        
    def _calculate_schemas_total_coupon_and_point(self, cr, uid, trans_id, context=None):
        _logger.info('Start Calculate Schemas Total Coupon and Point')        
        trans = self._get_trans(cr, uid, trans_id, context)
        customer_id = trans.customer_id        
        trans_schemas_ids = trans.trans_schemas_ids
        
        for trans_schemas_id in trans_schemas_ids:
            
            total_coupon = 0
            total_point = 0
            schemas_id = trans_schemas_id.schemas_id
            
            args = [('trans_schemas_id','=', trans_schemas_id.id)]  
            
            trans_coupon_ids = self.pool.get('rdm.trans.detail.coupon').search(cr, uid, args, context=context)
            trans_coupons = self.pool.get('rdm.trans.detail.coupon').browse(cr, uid, trans_coupon_ids, context=context)            
            for trans_coupon in trans_coupons:
                total_coupon = total_coupon + trans_coupon.valid_coupon
            
            _logger.info('Total Coupon for ' + str(trans_schemas_id.id) + ' : ' + str(total_coupon))
            
            trans_point_ids = self.pool.get('rdm.trans.detail.point').search(cr, uid, args, context=context)
            trans_points = self.pool.get('rdm.trans.detail.point').browse(cr, uid, trans_point_ids, context=context)
            for trans_point in trans_points:
                total_point = total_point + trans_point.valid_point
                
            _logger.info('Total Point for ' + str(trans_schemas_id.id) + ' : ' + str(total_point))    
            
            trans_schemas_data = {}
            if schemas_id.limit_coupon > -1 :
                if total_coupon > schemas_id.limit_coupon:
                    total_coupon = schemas_id.limit_coupon
                    
            trans_schemas_data.update({'total_coupon':total_coupon})
            
            #Check Limit Point Per Schemas
            #Customer without email will not get point
            if customer_id.email_required and customer_id.receive_email:
                if schemas_id.limit_point > -1 :
                    if total_point > schemas_id.limit_point:
                        total_point =  schemas_id.limit_point
            else:
                total_point = 0
                                                
            trans_schemas_data.update({'total_point':total_point})                      
            self.pool.get('rdm.trans.schemas').write(cr, uid, [trans_schemas_id.id], trans_schemas_data, context=context)
            
        _logger.info('End Calculate Schemas Total Coupon and Point')
        
    def _calculate_total_coupon_and_point(self, cr, uid, trans_id, context=None):
        _logger.info('Start Calculate Total Coupon and Point')
        
        ditotal_coupon = 0
        terbesar_coupon = 0
        ditotal_point = 0
        terbesar_point = 0
                
        total_coupon = 0
        total_point = 0
        
        trans = self._get_trans(cr, uid, trans_id, context)
        trans_schemas_ids = trans.trans_schemas_ids 
        
        for trans_schemas_id in trans_schemas_ids:  
            schemas_id = trans_schemas_id.schemas_id
            if schemas_id.calculation == 'ditotal':          
                ditotal_coupon = ditotal_coupon + trans_schemas_id.total_coupon
                ditotal_point = ditotal_point + trans_schemas_id.total_point
            if schemas_id.calculation == 'terbesar':
                if terbesar_coupon < trans_schemas_id.total_coupon:
                    terbesar_coupon = trans_schemas_id.total_coupon 
                if terbesar_point < trans_schemas_id.total_point:
                    terbesar_point = trans_schemas_id.total_point
                                    
        trans_data = {}
        trans_data.update({'total_coupon':ditotal_coupon + terbesar_coupon})                    
        trans_data.update({'total_point':ditotal_point + terbesar_point})                                                    
        self.pool.get('rdm.trans').write(cr, uid, [trans.id], trans_data, context=context)
        _logger.info('End Calculate Total Coupon and Point')        

    def _generate_coupon(self, cr, uid, trans_id, context=None):
        _logger.info('Start Generate Coupon')
        trans = self._get_trans(cr, uid, trans_id, context)
        trans_schemas_ids = trans.trans_schemas_ids
        for trans_schemas_id in trans_schemas_ids:
            _logger.info('Trans_ Schemas Total Coupon :' + str(trans_schemas_id.total_coupon))
            schemas_id = trans_schemas_id.schemas_id            
            coupon_data = {}
            coupon_data.update({'customer_id':trans.customer_id.id})
            coupon_data.update({'trans_id':trans.id})
            coupon_data.update({'trans_type':'promo'})        
            coupon_data.update({'coupon':trans_schemas_id.total_coupon})
            coupon_data.update({'expired_date':schemas_id.end_date})
            self.pool.get('rdm.customer.coupon').create(cr, uid, coupon_data, context=context)
        _logger.info('End Generate Coupon')
            
    def _generate_point(self, cr, uid, trans_id, context=None):
        _logger.info('Start Generate Point')
        trans = self._get_trans(cr, uid, trans_id, context)
        trans_schemas_ids = trans.trans_schemas_ids
        for trans_schemas_id in trans_schemas_ids:
            schemas_id = trans_schemas_id.schemas_id
            _logger.info('Total Point :' + str(trans_schemas_id.total_point))
            point_data = {}
            point_data.update({'customer_id': trans.customer_id.id})
            point_data.update({'trans_id':trans.id})
            point_data.update({'trans_type': 'promo'})
            point_data.update({'point':trans_schemas_id.total_point})
            point_data.update({'expired_date': schemas_id.point_expired_date})
            self.pool.get('rdm.customer.point').create(cr, uid, point_data, context=context)
        _logger.info('End Generate Coupon')
    
    def _generate_reward(self, cr, uid, trans_id, context=None):
        _logger.info('Start Generate Reward')
        pass
        _logger.info('End Generate Reward')
        
    def _define_trans_schemas(self, cr, uid, ids, context=None):
        trans_id  = ids[0]
        trans = self._get_trans(cr, uid, trans_id, context)
        if trans.type == 'promo':            
            active_schemas = self.pool.get('rdm.schemas').active_promo_schemas(cr, uid, context)
        if trans.type == 'point':
            active_schemas = self.pool.get('rdm.schemas').active_point_schemas(cr, uid, context)
            
        for schemas in active_schemas:            
            trans_schemas_data = {}
            trans_schemas_data.update({'trans_id': ids[0]})
            trans_schemas_data.update({'schemas_id': schemas.id})
            trans_schemas_id = self.pool.get('rdm.trans.schemas').create(cr, uid, trans_schemas_data, context=context)            
            self._get_customer_filters(cr, uid, ids, trans_schemas_id, context)
            self._get_valid_total(cr, uid, ids, trans_schemas_id, context)            
        
    def _pre_calculation(self, cr, uid, ids, context=None):
        trans_id = ids[0]
        #Calculate Total Amount
        self._get_total_amount(cr, uid, ids, context)
        #Check Filter for Active Schemas        
        self._define_trans_schemas(cr, uid, ids, context)
        
            
    def _post_calculation(self, cr, uid, ids, context=None):
        trans_id = ids[0]
        #Calculate Additional Coupon and Point for All Transaction Detail
        self._calculate_add_coupon_and_point(cr, uid, trans_id, context=context)
        #Calculate Priority for Coupon and Point
        self._calculate_trans_priority_per_schemas(cr, uid, trans_id, context)
        #Calculate Valid Coupon and Point
        self._calculate_valid_coupon_and_point(cr, uid, trans_id, context)
        #Close Trans Detail
        self._close_trans_detail(cr, uid, trans_id, context)
        #Calculate Schemas Total Coupon and Point          
        self._calculate_schemas_total_coupon_and_point(cr, uid, trans_id, context=context)        
        #Calculate Total Coupon and Point for All Schemas                                
        self._calculate_total_coupon_and_point(cr, uid, trans_id, context)
        #Generate Coupon
        self._generate_coupon(cr, uid, trans_id, context=context)
        #Generate Point
        self._generate_point(cr, uid, trans_id, context=context)
        #Send Email Notification
                                        
    def _send_email_notification(self, cr, uid, values, context=None):
        _logger.info('Start Send Email Notification')
        mail_mail = self.pool.get('mail.mail')
        mail_ids = []
        mail_ids.append(mail_mail.create(cr, uid, {
            'email_from': values['email_from'],
            'email_to': values['email_to'],
            'subject': values['subject'],
            'body_html': values['body_html'],
            }, context=context))
        mail_mail.send(cr, uid, mail_ids, context=context)
        _logger.info('End Send Email Notification')
    
    def send_mail_to_customer(self, cr, uid, ids, context=None):
        #res_id = self.read(cr, uid, ids, ['boq_item_ids'], context)[0]['id']
        trans_id = ids[0]
        trans = self._get_trans(cr, uid, trans_id, context)
        email_obj = self.pool.get('email.template')        
        template_ids = email_obj.search(cr, uid, [('name', '=', 'Redemption Trans Notification')])
        email = email_obj.browse(cr, uid, template_ids[0])  
        email_obj.write(cr, uid, template_ids, {'email_from': email.email_from,
                                                'email_to': email.email_to,
                                                'subject': email.subject,
                                                'body_html': email.body_html,
                                                'email_recipients': email.email_recipients})
        email_obj.send_mail(cr, uid, template_ids[0], trans.id, True, context=context)

    def current_total_amount(self, cr, uid, customer_id, context=None):
        today = datetime.datetime.now()
        sql_req = '''select sum(b.total_amount) as total_amount from rdm_trans a
                    left join rdm_trans_detail b on a.id = b.trans_id
                    left join rdm_customer c on a.customer_id = c.id                     
                    WHERE a.customer_id={0} AND a.trans_date = '{1}' AND b.state='done' '''.format(customer_id.id, today.strftime('%Y-%m-%d'))
        
        cr.execute(sql_req)
        sql_res = cr.dictfetchone()
        if sql_res:
            if sql_res['total_amount'] is not None:
                total_amount = sql_res['total_amount']
            else:
                total_amount = 0
        else:
            total_amount = 0                
        return total_amount
        
    def transactions_total_amount(self, cr, uid, trans_id, schemas_id, customer_id, context=None):
        
        today = datetime.datetime.now()
        sql_req = '''SELECT 
                    sum(rdm_trans_detail.total_amount) as total_amount
                 FROM 
                    public.rdm_trans, 
                    public.rdm_customer, 
                    public.rdm_trans_schemas, 
                    public.rdm_schemas,
                    public.rdm_trans_detail
                WHERE 
                  rdm_trans.customer_id = rdm_customer.id AND
                  rdm_trans_schemas.trans_id = rdm_trans.id AND
                  rdm_trans_schemas.schemas_id = rdm_schemas.id AND
                  rdm_trans_detail.trans_id = rdm_trans.id AND
                  rdm_customer.id = {0} AND                   
                  rdm_schemas.id = {1} AND
                  rdm_trans.trans_date = '{2}' AND
                  rdm_trans_detail.state = 'done' AND
                  rdm_trans.id != {3}                                
            '''.format(customer_id.id,schemas_id.id,today.strftime('%Y-%m-%d'), trans_id.id)

        cr.execute(sql_req)
        sql_res = cr.dictfetchone()
        if sql_res:
            if sql_res['total_amount'] is not None:
                total_amount = sql_res['total_amount']
            else:
                total_amount = 0
        else:
            total_amount = 0                
        return total_amount
        
    _columns = {
        'trans_id': fields.char('Transaction ID',size=13, readonly=True),
        'customer_id': fields.many2one('rdm.customer','Customer',required=True),
        'type': fields.selection([('promo','Promo'),('point','Point')],'Type',readonly=True),         
        'trans_date': fields.date('Date', required=True, readonly=True),
        'total_amount': fields.float('Total Amount', readonly=True),        
        'total_item': fields.integer('Total Item', readonly=True),
        'total_coupon': fields.integer('Total Coupon', readonly=True),
        'total_point': fields.integer('Total Point', readonly=True),          
        'state':  fields.selection(AVAILABLE_STATES, 'Status', size=16, readonly=True),
        'trans_detail_ids': fields.one2many('rdm.trans.detail','trans_id','Details'),
        'trans_detail_coupon_ids': fields.one2many('rdm.trans.detail.coupon','trans_id','Coupon Details'),
        'trans_detail_point_ids': fields.one2many('rdm.trans.detail.coupon','trans_id','Point Details'),        
        'trans_schemas_ids': fields.one2many('rdm.trans.schemas','trans_id','Schemas'),            
        'customer_coupon_ids': fields.one2many('rdm.customer.coupon','trans_id','Coupons'),
        'customer_point_ids': fields.one2many('rdm.customer.point','trans_id','Points'),
        'customer_reward_ids': fields.one2many('rdm.reward.trans','trans_id','Rewards'),        
        'remark': fields.text('Remark',readonly=True),
        'printed': fields.boolean('Printed', readonly=True),
        'reprint': fields.integer('Reprint', readonly=True),
        'reprint_remark': fields.text('Reprint Remark'),
        'deleted': fields.boolean('Deleted', readonly=True),
        'create_uid': fields.many2one('res.users','Created By', readonly=True),
        'create_date': fields.datetime('Date Created', readonly=True),
        'write_uid': fields.many2one('res.users','Modified By', readonly=True),
        'write_date': fields.datetime('Date Modified', readonly=True),
    }
       
    _defaults = {
        'trans_date': fields.date.context_today,                        
        'total_coupon': lambda *a: 0,
        'total_point': lambda *a: 0,      
        'state': lambda *a: 'draft',
        'printed': lambda *a: False,
        'reprint': lambda *a: 0,
        'deleted': lambda *a: False,
    }        
    
    _order = "create_date desc"
    
    def create(self, cr, uid, values, context=None):        
        values.update({'state':'open'})
        id = super(rdm_trans,self).create(cr, uid, values, context=context)
        ids = [id]
        #Generate and Set Transaction ID
        self._set_trans_id(cr, uid, ids, context)        
        #Process Calculation
        self._pre_calculation(cr, uid, ids, context)                        
        return id        
                 
    def write(self, cr, uid, ids, values, context=None ):
        trans_id = ids[0]                
        trans = self._get_trans(cr, uid, trans_id, context)        
        if trans.state == 'done':        
            _logger.info('State : Done')    
            if values.get('bypass') == True:
                _logger.info('Bypass Done State')
                trans_data = {}
                if values.get('method') == '_update_print_status':                                
                    trans_data.update({'printed':values.get('printed')})
                    super(rdm_trans,self).write(cr, uid, ids, trans_data, context=context)
                if values.get('method') == 'trans_reset':                                
                    trans_data.update({'state':values.get('state')})
                    super(rdm_trans,self).write(cr, uid, ids, trans_data, context=context)                            
                if values.get('method') == 'trans_req_delete':                                    
                    trans_data.update({'state':values.get('state')})
                    super(rdm_trans,self).write(cr, uid, ids, trans_data, context=context)                    
                    self.process_req_delete(cr, uid, ids, context)                                                
            else: 
                raise osv.except_osv(('Warning'), ('Edit not allowed, Transaction already closed!'))            
            
        if trans.state == 'open':   
            _logger.info('State : Open')                                 
            if values.get('state') == 'done':
                self.process_close(cr, uid, ids, context)
                super(rdm_trans,self).write(cr, uid, ids, values, context=context)
                #Calculate Total Amount
                self._get_total_amount(cr, uid, ids, context)                        
            else:
                super(rdm_trans,self).write(cr, uid, ids, values, context=context)
                #Calculate Total Amount
                self._get_total_amount(cr, uid, ids, context)
                                        
        if trans.state == 'req_delete':
            _logger.info('State : Request Delete')
            trans_data = {}
            trans_data.update({'state':values.get('state')})            
            if values.get('method') == 'trans_del_reject':
                super(rdm_trans,self).write(cr, uid, ids, trans_data, context=context)
                self.process_del_reject(cr, uid, ids, context)
            if values.get('method') == 'trans_del_approve':
                super(rdm_trans,self).write(cr, uid, ids, trans_data, context=context)
                self.process_del_approve(cr, uid, ids, context)                
                
        return True        

    def unlink(self, cr, uid, ids, context=None):
        data = {}
        data.update({'deleted': True})
        super(rdm_trans,self).write(cr, uid, ids, data, context=context)
                                    
rdm_trans()

class rdm_trans_detail(osv.osv):
    _name = "rdm.trans.detail"
    _description = "Redemption Promo Transaction Detail"
    
    def trans_close(self, cr, uid, ids, context=None):
        data = {}
        data.update({'state':'done'})
        self.write(cr, uid, ids, data, context=context)
        
    def trans_delete(self, cr, uid, ids, context=None):
        data = {}
        data.update({'state':'delete'})
        self.write(cr, uid, ids, data, context=context)
        
        
    def onchange_bank_id(self, cr, uid, ids, bank_id, context=None):
        _logger.info('Start Onchange Bank ID')
        return {'domain':{'bank_card_id':[('bank_id','=', bank_id)]}}        
        _logger.info('End Onchange Bank ID')
                    
    _columns = {
        'trans_id': fields.many2one('rdm.trans','Transaction', required=True),
        'tenant_id': fields.many2one('rdm.tenant','Tenant',required=True),
        'tenant_filter': fields.boolean('Tenant Filter'),        
        'trans_date': fields.date('Date',required=True),
        'total_amount': fields.float('Total Amount',required=True),
        'valid_amount': fields.float('Valid Amount', readonly=True),
        'total_item': fields.integer('Total Item'),
        'payment_type': fields.selection([('cash','Cash'),('creditcard','Credit Card'),('debit','Debit')],'Payment Type',required=True),
        'bank_id': fields.many2one('rdm.bank','Bank'),
        'bank_card_id': fields.many2one('rdm.bank.card','Bank Card'),                                
        'card_number': fields.char('Card Number', size=20),        
        'trans_detail_coupon_ids': fields.one2many('rdm.trans.detail.coupon','trans_detail_id','Coupons'),
        'trans_detail_point_ids': fields.one2many('rdm.trans.detail.point','trans_detail_id','Points'),                
        'state':  fields.selection(AVAILABLE_STATES, 'Status', size=16, readonly=True),
        'deleted': fields.boolean('Deleted'),      
    }
    
    _defaults = {
        'trans_date': fields.date.context_today, 
        'payment_type': lambda *a: 'cash',
        'tenant_filter': lambda *a: False,
        'state': lambda *a: 'open',
    }
    
    def unlink(self, cr, uid, ids, context=None):
        data = {}
        data.update({'deleted': True})
        super(rdm_trans_detail,self).write(cr, uid, ids, data, context=context)
                
rdm_trans_detail()

class rdm_trans_detail_coupon(osv.osv):
    _name = "rdm.trans.detail.coupon"
    _description = "Redemption Transaction Detail Coupon"


    def total_coupon(self, cr, uid, trans_schemas_id, context=None):        
        sql_req= "SELECT sum(c.coupon) as total FROM rdm_trans_detail_coupon c WHERE c.trans_schemas_id=" + str(trans_schemas_id)         
        cr.execute(sql_req)
        sql_res = cr.dictfetchone()
        total_coupon = sql_res['total']
        if total_coupon == None:
            total_coupon = 0    
        return total_coupon
            
    _columns = {
        'trans_id': fields.many2one('rdm.trans', 'Transaction'),
        'trans_detail_id': fields.many2one('rdm.trans.detail','Transaction Detail'),
        'trans_schemas_id': fields.many2one('rdm.trans.schemas','Transaction Schemas'),
        'priority': fields.integer('Priority'),
        'basic': fields.float('Basic'),            
        'coupon': fields.float('Coupon'),
        'valid_coupon': fields.float('Valid Coupon'),
        'multiple_ditotal': fields.float('Mutiple Ditotal'),
        'multiple_terbesar': fields.float('Mutiple Terbesar'),
        'add_ditotal': fields.float('Add Ditotal'),
        'add_terbesar': fields.float('Add Terbesar'),
        'state': fields.selection(AVAILABLE_STATES,'Status',size=16,readonly=True),
    }    
    
    _defaults = {
        'state': lambda *a: 'open',
    }
    
rdm_trans_detail_coupon()

class rdm_trans_detail_point(osv.osv):
    _name = "rdm.trans.detail.point"
    _description = "Redemption Transaction Detail Point"
    
    def total_point(self, cr, uid, trans_schemas_id, context=None):        
        sql_req= "SELECT sum(c.point) as total FROM rdm_trans_detail_point c WHERE c.trans_schemas_id=" + str(trans_schemas_id)         
        cr.execute(sql_req)
        sql_res = cr.dictfetchone()
        total_point = sql_res['total']
        if total_point == None:
            total_point = 0    
        return total_point
    
    _columns = {
        'trans_id': fields.many2one('rdm.trans', 'Transaction'),        
        'trans_detail_id': fields.many2one('rdm.trans.detail','Transaction Detail'),
        'trans_schemas_id': fields.many2one('rdm.trans.schemas','Transaction Schemas'),
        'priority': fields.integer('Priority'),
        'basic': fields.float('Basic'),
        'point': fields.float('Point'),
        'valid_point': fields.float('Valid Point'),
        'multiple_ditotal': fields.float('Mutiple Ditotal'),
        'multiple_terbesar': fields.float('Mutiple Terbesar'),
        'add_ditotal': fields.float('Add Ditotal'),
        'add_terbesar': fields.float('Add Terbesar'),     
        'state': fields.selection(AVAILABLE_STATES,'Status',size=16,readonly=True),   
    }    
    
    _defaults = {
        'state': lambda *a: 'open',
    }
    
rdm_trans_detail_point()


class rdm_trans_detail_reward(osv.osv):
    _name = "rdm.trans.detail.reward"
    _description = "Redemption Transaction Detail Reward"
    _columns = {
        'trans_detail_id': fields.many2one('rdm.trans.detail','Transaction Detail'),
        'trans_schemas_id': fields.many2one('rdm.trans.schemas','Transaction Schemas'),        
        'reward_id': fields.many2one('rdm.reward','Reward'),
        'quantity': fields.integer('Quantity'),
    }    
rdm_trans_detail_reward()

class rdm_trans_schemas(osv.osv):
    _name = "rdm.trans.schemas"
    _description = "Redemption Transaction Schemas"
    _columns = {
        'trans_id': fields.many2one('rdm.trans','Transaction', required=True),
        'schemas_id': fields.many2one('rdm.schemas','Schemas', required=True),
        'total_coupon': fields.integer('Total Coupon', readonly=True),
        'total_point': fields.integer('Total Point', readonly=True),
        'trans_filter': fields.boolean('Filter', readonly=True),
        'trans_valid': fields.boolean('Valid', readonly=True),
        'remark': fields.text('Remark',readonly=True),        
        'trans_detail_coupon_ids': fields.one2many('rdm.trans.detail.coupon','trans_schemas_id','Schemas Coupon'),
        'trans_detail_point_ids': fields.one2many('rdm.trans.detail.point','trans_schemas_id','Schemas Point'),                        
        'state': fields.selection(AVAILABLE_STATES,'Status', size=16, readonly=True),
    }
    _defaults = {
        'total_coupon': lambda *a: 0,
        'total_point': lambda *a: 0,
        'trans_filter': lambda *a: False,
        'trans_valid': lambda *a: False,
    }
    
rdm_trans_schemas()
