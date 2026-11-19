"""字幕事件源、归档快照及编辑引用登记台。"""
from .common import *
import time
class App(Application):
    def handle(self,h):
        p=h.path_only
        if p=='/': return self.document(h,'讲座字幕源','<p>场次 TALK-1</p><a href="/snapshot/TALK-1">归档快照</a><p>实时流 /stream/TALK-1，说明见来源契约。</p>')
        if p=='/snapshot/TALK-1':
            watermark=self.data('snapshot_watermark')
            return self.emit(h,{'talk':'TALK-1','covers_through':watermark,'events':[e for e in self.seed['events'] if e['id']<=watermark]})
        if p=='/stream/TALK-1':
            start=int(h.headers.get('Last-Event-ID','0'))
            if start<self.data('floor'): return self.emit(h,{'error':'replay_expired','floor':self.data('floor'),'snapshot':'/snapshot/TALK-1'},409)
            h.send_response(200);h.send_header('Content-Type','text/event-stream; charset=utf-8');h.send_header('Connection','close');h.send_header('Cache-Control','no-store');h.end_headers();h.close_connection=True
            for event in self.seed['events']:
                if start<event['id']<=self.data('tail'):
                    value=dump({k:v for k,v in event.items() if k not in ['id','type']})
                    # 一个JSON对象在字段逗号处分成两条data行，符合SSE换行拼接规则。
                    cut=value.find(',')+1
                    chunk=(': 心跳\r\nid: '+str(event['id'])+'\r\nevent: '+event['type']+'\r\ndata: '+value[:cut]+'\r\ndata: '+value[cut:]+'\r\n\r\n').encode()
                    try:
                        for index in range(0,len(chunk),7): h.wfile.write(chunk[index:index+7]);h.wfile.flush()
                        time.sleep(.005)
                    except (BrokenPipeError,ConnectionResetError): break
            return
        if p=='/editor/exports':
            if h.command=='POST':
                item=self.record('exports',h.body['reference'],h.body)
                return self.emit(h,item)
            return self.emit(h,self.remote.get('exports',{}))
        if p=='/editor/withdrawals':
            if h.command=='POST': return self.emit(h,self.record('withdrawals',h.body['reference'],h.body))
            return self.emit(h,self.remote.get('withdrawals',{}))
        return self.emit(h,{'error':'not_found'},404)
if __name__=='__main__': main(App())
