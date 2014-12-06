from openerp.osv import fields, osv
import datetime

import logging
_logger = logging.getLogger(__name__)

AVAILABLE_STATES = [
    ('draft','New'),
    ('request','Request'),
    ('ready','Ready'),    
    ('open','Open'),    
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
        params = self.browse(cr, uid, ids, context=context)
        param = params[0]   
        serverUrl = 'http://' + reportserver + ':' + reportserverport +'/jasperserver'
        j_username = 'rdm_operator'
        j_password = 'rdm123'
        ParentFolderUri = '/rdm'
        reportUnit = '/rdm/rdm_trans_receipt_report'
        url = serverUrl + '/flow.html?_flowId=viewReportFlow&standAlone=true&_flowId=viewReportFlow&ParentFolderUri=' + ParentFolderUri + '&reportUnit=' + reportUnit + '&ID=' +  param.id + '&decorate=no&j_username=' + j_username + '&j_password=' + j_password
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
        _logger.info("Close Transaction for ID : " + str(ids))    
        self.write(cr,uid,ids,{'state':'done'},context=context)      
        self._calculate_coupon_and_point(cr, uid, ids[0], context)   
        self._calculate_add_coupon_and_point(cr, uid, ids[0], context)
        self._calculate_total_coupon_and_point(cr, uid, ids[0], context)
        self._generate_coupon(cr, uid, ids[0], context)     
        self._generate_point(cr, uid, ids[0], context)
        return True
    
    def _update_print_status(self, cr, uid, ids, context=None):
        _logger.info("Update Print Status for ID : " + str(ids))
        values = {}
        values.update({'bypass':True})
        values.update({'method':'_update_print_status'})
        values.update({'printed':True})
        self.write(cr, uid, ids, values, context=context)
                
    def print_receipt(self, cr, uid, ids, context=None):
        _logger.info("Print Receipt for ID : " + str(ids))
        self._update_print_status(cr, uid, ids, context)        
        id = ids[0]   
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
        _logger.info("Reset for ID : " + str(ids))
        return True
    
    def trans_req_delete(self, cr, uid, ids, context=None):
        #self.write(cr,uid,ids,{'reg_delete':'done'},context=context)
        trans_id = ids[0]
        trans = self._get_trans(cr, uid, trans_id, context)
        trans_detail_ids = trans.trans_detail_ids
        for trans_detail in trans_detail_ids:
            self.pool.get('rdm.trans.detail').write(cr, uid, trans_detail.id, {'state':'req_delete'})
            
        customer_coupon_ids = self.pool.get('rdm.customer.coupon').search(cr, uid, [('trans_id','=',trans_id)],context=context)
        self.pool.get('rdm.customer.coupon').write(cr, uid, customer_coupon_ids[0], {'state':'req_delete'})
        customer_point_ids = self.pool.get('rdm.customer.point').search(cr, uid, [('trans_id','=',trans_id)],context=context)
        self.pool.get('rdm.customer.point').write(cr, uid, customer_point_ids[0], {'state':'req_delete'})
        return True
        
    def trans_del_approve(self, cr, uid, ids, context=None):
        trans_id = ids[0]
        trans = self._get_trans(cr, uid, trans_id, context)
        rdm_config = self.pool.get('rdm.config').get_config(cr, uid, context=context)
        if rdm_config.trans_delete_approver.user_id == uid:
            trans_detail_ids = trans.trans_detail_ids
            for trans_detail in trans_detail_ids:
                self.pool.get('rdm.trans.detail').write(cr, uid, trans_detail.id, {'state':'delete'})
            
            customer_coupon_ids = self.pool.get('rdm.customer.coupon').search(cr, uid, [('trans_id','=',trans_id)],context=context)
            self.pool.get('rdm.customer.coupon').write(cr, uid, customer_coupon_ids[0], {'state':'delete'})
            customer_point_ids = self.pool.get('rdm.customer.point').search(cr, uid, [('trans_id','=',trans_id)],context=context)
            self.pool.get('rdm.customer.point').write(cr, uid, customer_point_ids[0], {'state':'delete'})            
        else:
            raise osv.except_osv(('Warning'), ('Approve Process not allowed!')) 
        
        
    def trans_del_reject(self, cr, uid, ids, context=None):
        trans_id = ids[0]
        trans = self._get_trans(cr, uid, trans_id, context)
        rdm_config = self.pool.get('rdm.config').get_config(cr, uid, context=context)
        if rdm_config.trans_delete_approver.user_id == uid:
            trans_detail_ids = trans.trans_detail_ids
            for trans_detail in trans_detail_ids:
                self.pool.get('rdm.trans.detail').write(cr, uid, trans_detail.id, {'state':'done'})
            
            customer_coupon_ids = self.pool.get('rdm.customer.coupon').search(cr, uid, [('trans_id','=',trans_id)],context=context)
            self.pool.get('rdm.customer.coupon').write(cr, uid, customer_coupon_ids[0], {'state':'done'})
            customer_point_ids = self.pool.get('rdm.customer.point').search(cr, uid, [('trans_id','=',trans_id)],context=context)
            self.pool.get('rdm.customer.point').write(cr, uid, customer_point_ids[0], {'state':'done'})            
        else:
            raise osv.except_osv(('Warning'), ('Reject Process not allowed!')) 
        
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
            
    def _get_trans_detail(self, cr, uid, trans_id, context=None):
        return self.pool.get('rdm.trans.detail').browse(cr, uid, trans_id, context=context)
                          
    def _get_schemas_rules(self, cr, uid, schemas_id, context=None):
        ids = self.pool.get('rdm.schemas.rules').search(cr, uid, [('schemas_id','=',schemas_id)], context=context);
        return self.pool.get('rdm.schemas.rules').browse(cr, uid, ids, context=context)
    
    def _get_customer_filters(self, cr, uid, trans_id, context=None):                
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
       
        trans = self._get_trans(cr, uid, trans_id, context)
        schemas = trans.schemas_id
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
        if schemas.religion_ids:
            for schemas_religion_id in schemas.religion_ids:
                if schemas_religion_id.religion_id.id == customer.religion.id:
                    religion_message = "Religion Allowed"                                                
                    religion_status = True                
        else:
            religion_message = "Religion Allowed"                                                
            religion_status = True
        
        #Filter Ethnic     
        if schemas.ethnic_ids:
            for schemas_ethnic_id in schemas.ethnic_ids:
                if schemas_ethnic_id.ethnic_id.id == customer.ethnic.id:
                    ethnic_message = "Ethnic Allowed"                                                
                    ethnic_status = True
        else:
            ethnic_message = "Ethnic Allowed"                                                
            ethnic_status = True
        
        #Filter Marital
        if schemas.marital_ids:
            for schemas_marital_id in schemas.marital_ids:
                if schemas_marital_id.marital_id.id == customer.marital.id:
                    marital_message = "Marital Allowed"                                                
                    marital_status = True
        else:
            marital_message = "Marital Allowed"                                                
            marital_status = True
            
        #Filter Interest  
        if schemas.interest_ids:
            for schemas_interest_id in schemas.interest_ids:
                if schemas_interest_id.interest_id.id == customer.interest.id:
                    interest_message = "Interest Allowed"                                                
                    interest_status = True                    
        else: 
            interest_message = "Interest Allowed"                                                
            interest_status = True                
        
        #Filter AYC Card Type
        if schemas.card_type_ids:
            for schemas_card_type_id in schemas.card_type_ids:
                if schemas_card_type_id.card_type_id.id == customer.card_type.id:
                    cardtype_message = "Card Type Allowed"                                                
                    cardtype_status = True                
        else:
            cardtype_message = "Card Type Allowed"                                                
            cardtype_status = True                             
            
        status = segment_status and gender_status and religion_status and ethnic_status and marital_status and interest_status and cardtype_status
        message = segment_message + "\n" + gender_message + "\n" + religion_message + "\n" + ethnic_message + "\n" + marital_message + "\n" + interest_message + "\n" + cardtype_message
        datas = {}
        if status == True:
            datas.update({'trans_filter':True})                                    
        datas.update({'remark': message})
        super(rdm_trans,self).write(cr, uid, [trans_id], datas, context=context)            
        return None    
    
    def _get_tenant_filters(self, cr, uid, schemas_id, tenant_id, context=None):
        _logger.info('Start Tenant Filter')
        status = False
        message = "Error tenant " + str(tenant_id.id) + " filter"
        
        schemas_tenant_ids = schemas_id.tenant_ids
        if schemas_tenant_ids:
            for schemas_tenant_id in schemas_tenant_ids:
                if schemas_tenant_id.tenant_id.id == tenant_id.id:
                    status = True
        else:
            status = True
            
        schemas_tenant_category_ids = schemas_id.tenant_category_ids
        if schemas_tenant_category_ids:
            for schemas_tenant_category_id in schemas_tenant_category_ids:
                if schemas_tenant_category_id.tenant_category_id.id == tenant_id.category.id:
                    status = True
        else:
            status = True                
                                                
        _logger.info('End Tenant Filter')                            
        return status, message    
    
    def _set_trans_id(self, cr, uid, trans_id, context=None):
        _logger.info('Start Set Trans ID Filter')
        trans = self._get_trans(cr, uid, trans_id, context)
        if trans.type == 'promo':
            trans_seq_id = self.pool.get('ir.sequence').get(cr, uid, 'rdm.trans.redemption.sequence'),
        if trans.type == 'point':
            trans_seq_id = self.pool.get('ir.sequence').get(cr, uid, 'rdm.trans.point.sequence'),            
        trans_data = {}
        trans_data.update({'trans_id':trans_seq_id[0]})        
        super(rdm_trans,self).write(cr, uid, [trans_id], trans_data, context=context)
        _logger.info('End Set Trans ID Filter')
        
    
    def _get_total(self, cr, uid, trans_id, context=None):
        _logger.info('Start Get Total Filter')        
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

    def _get_valid_total(self, cr, uid, trans_id, context=None):
        _logger.info('Start Get Valid Total Filter')        
        trans = self._get_trans(cr, uid, trans_id, context)
        schemas_id = trans.schemas_id
        valid_amount = 0                    
        for trans_detail in trans.trans_detail_ids:
            tenant_id = trans_detail.tenant_id
            status, message = self._get_tenant_filters(cr, uid, schemas_id, tenant_id, context=context)
            if status:        
                valid_amount = valid_amount + trans_detail.total_amount
            else:
                detail_data = {}
                detail_data.update({'tenant_filter':True})
                self.pool.get('rdm.trans.detail').write(cr, uid, [trans_detail.id], detail_data, context=context)             
            
        trans_data = {}
        if trans.trans_filter == True:
            trans_data.update({'valid_amount':valid_amount})
        else:
            trans_data.update({'valid_amount':0})        
        super(rdm_trans,self).write(cr, uid, [trans_id], trans_data, context=context)
        _logger.info('End Get Total Filter')
        
    
    def _calculate_coupon_and_point(self,cr, uid, trans_id, context=None):
        _logger.info('Start Calculate Coupon and Point')
        coupon = 0
        point = 0       
        trans = self._get_trans(cr, uid, trans_id, context)
        schemas_id = trans.schemas_id
        valid_amount = trans.valid_amount
             
        if trans.type == 'promo':                        
            coupon = (valid_amount // schemas_id.spend_amount) * schemas_id.coupon
            
        if schemas_id.limit_point == -1:
            point = (valid_amount // schemas_id.spend_amount) * schemas_id.point
        else:
            point = (valid_amount // schemas_id.spend_amount) * schemas_id.point
            if point > schemas_id.limit_point:
                point = schemas_id.limit_point
                            
        trans_data = {}
        trans_data.update({'coupon':coupon})
        trans_data.update({'point':point})
        
        super(rdm_trans,self).write(cr, uid, [trans_id], trans_data, context=context)
        _logger.info('End Calculate Coupon and Point')
    
    def _calculate_add_coupon_and_point(self, cr, uid, trans_id, context=None):
        _logger.info('Start Calculate Add Coupon and Point')
        coupon = 0
        point = 0        
        trans = self._get_trans(cr, uid, trans_id, context)
        schemas_id = trans.schemas_id
        customer_id = trans.customer_id        
        rules_ids = schemas_id.rules_ids
        for rules_id in rules_ids:
            #Check Rules Schemas
            #Birthday Schemas
            rules = rules_id.rules_id
            if rules.rule_schema == 'birthday':
                _logger.info('Start Birthday Schemas')
                today = datetime.date.today().strftime("%Y-%m-%d")
                _logger.info('Today : ' + today)
                birthdate = customer_id.birth_date
                _logger.info('Birth Date : ' + birthdate)
                _logger.info('Start Birthday Schemas')
                if today == birthdate :
                    _logger.info('Rules Birthday Match')
                    if rules.apply_for == '1':
                        if rules.operation == 'add':
                            coupon = coupon + rules.quantity
                        if rules.operation == 'multiple':
                            coupon = coupon + rules.quantity
                                                
                    if rules.apply_for == '2':
                        if rules.operation == 'add':
                            point = point + rules.quantity
                        if rules.operation == 'multiple':
                            point = point + rules.quantity
                                                        
                _logger.info(today)                
                _logger.info('End Birthday Schemas')
                                
            #Day Schemas
            if rules.rule_schema == 'day':
                _logger.info('Start Day Schemas')                
                today = datetime.date.today().strftime("%Y-%m-%d")
                day = rules.day
                if today == day :
                    if rules.apply_for == '1':
                        if rules.operation == 'add':
                            coupon = coupon + rules.quantity
                        if rules.operation == 'multiple':
                            coupon = coupon + rules.quantity
                                                
                    if rules.apply_for == '2':
                        if rules.operation == 'add':
                            point = point + rules.quantity
                        if rules.operation == 'multiple':
                            point = point + rules.quantity

                _logger.info('End Day Schemas')
            #Day Name Schemas
            if rules.rule_schema == 'dayname':
                _logger.info('Start Day Name Schemas')
                dayname = datetime.date.weekday()
                
                _logger.info('Start Day Name Schemas')
                
            #Card Type
            if rules.rule_schema == 'cardtype':
                card_type_rules = False
                _logger.info('Start Card Type Schemas')
                customer_card_type = customer_id.card_type
                card_type_ids = rules.card_type_ids
                for card_type in card_type_ids:
                    if customer_card_type.id == card_type.id:
                        card_type_rules = True
                if card_type_rules == True:
                    if rules.apply_for == '1':
                        if rules.operation == 'add':
                            coupon = coupon + rules.quantity
                        if rules.operation == 'multiple':
                            coupon = coupon + rules.quantity
                                                
                    if rules.apply_for == '2':
                        if rules.operation == 'add':
                            point = point + rules.quantity
                        if rules.operation == 'multiple':
                            point = point + rules.quantity                                                                
                _logger.info('Start Card Type Schemas')
                
            #Tenant Type
            if rules.rule_schema == 'tenanttype':
                _logger.info('Start Tenant Type Schemas')
                total_amount = 0                
                trans_detail_ids = trans.trans_detail_ids
                for trans_detail in trans_detail_ids:
                    #Get Tenant Type Information
                    tenant_id = trans_detail.tenant_id                                    
                    tenant_category_id = tenant_id.category.id
                    _logger.info('Tenant Type ID : ' + str(tenant_category_id))
                    #Get Tenant Type IDS from Schemas
                    rules_tenant_category_ids = rules.tenant_category_ids                                                    
                    _logger.info('Length Schemas Tenant Type : ' + str(len(rules_tenant_category_ids)))
                    for rules_tenant_category in rules_tenant_category_ids:                        
                        _logger.info('Schemas Tenant Type ID : ' + str(rules_tenant_category.tenant_category_id.id))
                        if rules_tenant_category.tenant_category_id.id == tenant_category_id:
                            total_amount = total_amount + trans_detail.total_amount
                            
                _logger.info('Total Amount : ' + str(total_amount))
                spend_amount = schemas_id.spend_amount
                _logger.info('Spend Amount : ' + str(spend_amount))
                
                if total_amount > spend_amount:
                    if rules.apply_for == '1':
                        if rules.operation == 'add':
                            add_coupon = (total_amount // schemas_id.spend_amount) + schemas_id.coupon
                            coupon = coupon + add_coupon
                        if rules.operation == 'multiple':
                            add_coupon = (total_amount // schemas_id.spend_amount) * schemas_id.coupon
                            coupon = coupon + add_coupon
                                                
                    if rules.apply_for == '2':
                        if rules.operation == 'add':
                            point = point + rules.quantity
                        if rules.operation == 'multiple':
                            point = point + rules.quantity
                                                                                                                                                                        
                _logger.info('End Tenant Type Schemas')
                
            #Bank Card
            if rules.rule_schema == 'bankcard':
                _logger.info('Start Bank Card Schemas')
                total_amount = 0
                rules_bank_ids = rules.bank_ids
                bank_card_list = {}
                for rules_bank in rules_bank_ids:
                    rules_bank_id = rules_bank.bank_id.id
                    rules_bank_name = rules_bank.bank_id.name
                    bank_card_list.update({rules_bank_id:rules_bank_name})
                    
                trans_detail_ids = trans.trans_detail_ids
                for trans_detail in trans_detail_ids:                    
                    if trans_detail.payment_type == 'creditcard' or trans_detail.payment_type == 'debit':
                        bank_id  =  trans_detail.bank_id
                        if bank_id.id in bank_card_list.keys():
                            total_amount = total_amount + trans_detail.total_amount
                _logger.info('Total Amount : ' + str(total_amount))
                
                if total_amount > spend_amount:
                    if rules.apply_for == '1':
                        if rules.operation == 'add':
                            add_coupon = (total_amount // schemas_id.spend_amount) * rules.quantity
                            coupon = coupon + add_coupon
                        if rules.operation == 'multiple':
                            add_coupon = (total_amount // schemas_id.spend_amount) * rules.quantity
                            coupon = coupon + add_coupon
                                                
                    if rules.apply_for == '2':
                        if rules.operation == 'add':
                            add_coupon = rules.quantity
                            point = point + add_coupon
                        if rules.operation == 'multiple':
                            add_coupon = (total_amount // schemas_id.spend_amount) * rules.quantity
                            point = point + add_coupon                                                                                   
                    
                    coupon = coupon + add_coupon
                    _logger.info('Coupon : ' + str(coupon))
                        
                _logger.info('End Bank Card Schemas')        
                
        trans_data = {}
        trans_data.update({'add_coupon':coupon})
        trans_data.update({'add_point':point})
        
        super(rdm_trans,self).write(cr, uid, [trans_id], trans_data, context=context)        
        _logger.info('End Calculate Add Coupon and Point')
                    
    def _calculate_total_coupon_and_point(self, cr, uid, trans_id, context=None):
        _logger.info('Start Calculate Total Coupon and Point')
        trans = self._get_trans(cr, uid, trans_id, context)
        total_coupon = trans.coupon + trans.add_coupon
        total_point = trans.point + trans.add_point
        
        trans_data = {}
        trans_data.update({'total_coupon':total_coupon})
        trans_data.update({'total_point':total_point})
        
        super(rdm_trans,self).write(cr, uid, [trans_id], trans_data, context=context)
        _logger.info('End Calculate Total Coupon and Point')
        
    def _generate_coupon(self, cr, uid, trans_id, context=None):
        _logger.info('Start Generate Coupon')
        trans = self._get_trans(cr, uid, trans_id, context)
        schemas_id = trans.schemas_id
        _logger.info('Total Coupon :' + str(trans.total_coupon))
        coupon_data = {}
        coupon_data.update({'customer_id':trans.customer_id.id})
        coupon_data.update({'trans_id':trans.id})
        coupon_data.update({'trans_type':'promo'})        
        coupon_data.update({'coupon':trans.total_coupon})
        coupon_data.update({'expired_date':schemas_id.end_date})
        customer_coupon_id = self.pool.get('rdm.customer.coupon').create(cr, uid, coupon_data, context=context)                    
        for i in range (0,trans.total_coupon):
            coupon_detail = {}
            coupon_code = self.pool.get('ir.sequence').get(cr, uid, 'rdm.customer.coupon.sequence') 
            _logger.info('Coupon ID : ' + coupon_code)     
            coupon_detail.update({'customer_coupon_id':customer_coupon_id}) 
            coupon_detail.update({'coupon_code':coupon_code})                        
            coupon_detail.update({'expired_date':schemas_id.end_date})        
            _logger.info('End Date : ' + schemas_id.end_date)            
            self.pool.get('rdm.customer.coupon.detail').create(cr, uid, coupon_detail, context=context)
        _logger.info('End Generate Coupon')
            
    def _generate_point(self, cr, uid, trans_id, context=None):
        _logger.info('Start Generate Point')
        trans = self._get_trans(cr, uid, trans_id, context)
        schemas_id = trans.schemas_id
        _logger.info('Total Point :' + str(trans.total_point))
        point_data = {}
        point_data.update({'customer_id': trans.customer_id.id})
        point_data.update({'trans_id':trans.id})
        point_data.update({'trans_type': 'promo'})
        point_data.update({'point':trans.total_point})
        point_data.update({'expired_date': schemas_id.end_date})
        self.pool.get('rdm.customer.point').create(cr, uid, point_data, context=context)
        _logger.info('End Generate Coupon')
    
    _columns = {
        'trans_id': fields.char('Transaction ID',size=13, readonly=True),
        'customer_id': fields.many2one('rdm.customer','Customer',required=True),
        'type': fields.selection([('promo','Promo'),('point','Point')],'Type',readonly=True), 
        'schemas_id': fields.many2one('rdm.schemas','Schemas', readonly=True),        
        'trans_date': fields.date('Date', required=True, readonly=True),
        'total_amount': fields.float('Total Amount', readonly=True),
        'valid_amount': fields.float('Valid Amount', readonly=True),
        'total_item': fields.integer('Total Item', readonly=True),  
        'total_coupon': fields.integer('Total Coupon', readonly=True),
        'total_point': fields.integer('Total Point', readonly=True),
        'coupon': fields.integer('Coupon', readonly=True),
        'point': fields.integer('Point', readonly=True),
        'add_coupon': fields.integer('Additional Coupon', readonly=True),
        'add_point': fields.integer('Additional Point', readonly=True),
        'trans_filter': fields.boolean('Trans Filter', readonly=True),
        'state':  fields.selection(AVAILABLE_STATES, 'Status', size=16, readonly=True),
        'trans_detail_ids': fields.one2many('rdm.trans.detail','trans_id','Details'),
        'customer_coupon_ids': fields.one2many('rdm.customer.coupon','trans_id','Coupons'),
        'customer_point_ids': fields.one2many('rdm.customer.point','trans_id','Points'),        
        'remark': fields.text('Remark',readonly=True),
        'printed': fields.boolean('Printed', readonly=True),
        'reprint': fields.integer('Reprint', readonly=True),
        'reprint_remark': fields.text('Reprint Remark'),
        'deleted': fields.boolean('Deleted', readonly=True),
    }
       
    _defaults = {
        'trans_date': fields.date.context_today,        
        'schemas_id': _get_active_schemas,        
        'trans_filter': lambda *a: False,        
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
        trans_id = super(rdm_trans,self).create(cr, uid, values, context=context)
        #Set trans_id
        self._set_trans_id(cr, uid, trans_id, context)
        #Calculate Total Amount and Total Item
        self._get_total(cr, uid, trans_id, context);        
        #Checks Customer Filters
        self._get_customer_filters(cr, uid, trans_id, context)
        #Calculate Valid Total Amount
        self._get_valid_total(cr, uid, trans_id, context)                
        return trans_id        
                 
    def write(self, cr, uid, ids, values, context=None ):
        trans_id = ids[0]                
        trans = self._get_trans(cr, uid, trans_id, context)        
        if trans['state'] == 'done':            
            if values.get('bypass') == True:
                trans_data = {}
                if values.get('method') == '_update_print_status':                                
                    trans_data.update({'printed':values.get('printed')})
                    result = super(rdm_trans,self).write(cr, uid, ids, trans_data, context=context)
            else: 
                raise osv.except_osv(('Warning'), ('Edit not allowed, Transaction already closed!'))            
        else:
            result = super(rdm_trans,self).write(cr, uid, ids, values, context=context)
            #Calculate Total Amount and Total Item
            self._get_total(cr, uid, trans_id, context);
            #Calculate Valid Total Amount
            self._get_valid_total(cr, uid, trans_id, context) 
            return result        

    def unlink(self, cr, uid, ids, context=None):
        data = {}
        data.update({'deleted': True})
        super(rdm_trans,self).write(cr, uid, ids, data, context=context)
                                    
rdm_trans()

class rdm_trans_detail(osv.osv):
    _name = "rdm.trans.detail"
    _description = "Redemption Promo Transaction Detail"
    
    _columns = {
        'trans_id': fields.many2one('rdm.trans','Transaction', required=True),
        'tenant_id': fields.many2one('rdm.tenant','Tenant',required=True),
        'tenant_filter': fields.boolean('Tenant Filter'),        
        'trans_date': fields.date('Date',required=True),
        'total_amount': fields.float('Total Amount',required=True),
        'total_item': fields.integer('Total Item'),
        'payment_type': fields.selection([('cash','Cash'),('creditcard','Credit Card'),('debit','Debit')],'Payment Type',required=True),
        'bank_id': fields.many2one('rdm.bank','Bank'),
        'bank_card_type': fields.selection([('silver','Silver'),('gold','Gold')],'Bank Card Type'),                
        'card_provider': fields.selection([('visa','Visa'),('master','Master')],'Card Provider'),        
        'card_number': fields.char('Card Number', size=20),          
        'state':  fields.selection(AVAILABLE_STATES, 'Status', size=16, readonly=True),
        'deleted': fields.boolean('Deleted'),      
    }
    
    _defaults = {
        'trans_date': fields.date.context_today, 
        'payment_type': lambda *a: 'cash',
        'tenant_filter': lambda *a: False,
    }
    
    def unlink(self, cr, uid, ids, context=None):
        data = {}
        data.update({'deleted': True})
        super(rdm_trans_detail,self).write(cr, uid, ids, data, context=context)
                
rdm_trans_detail()