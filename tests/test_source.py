"""真实流连接的分行、接续窗口与归档水位契约。"""
from support import *
class Protocol(SourceCase):
    def test_sse_wire_and_expired_replay(self):
        status,headers,data=self.request('/stream/TALK-1');self.assertEqual(status,200);self.assertIn('text/event-stream',headers['Content-Type']);self.assertIn('心跳'.encode(),data);self.assertEqual(data.count(b'\r\ndata: '),16);self.assertIn(b'id: 8\r\n',data)
        snap=self.value('/snapshot/TALK-1');self.assertEqual(snap['covers_through'],4);self.assertEqual(len(snap['events']),4)
        self.control(1);self.assertEqual(self.request('/stream/TALK-1',headers={'Last-Event-ID':'4'})[0],409);status,_,data=self.request('/stream/TALK-1',headers={'Last-Event-ID':'8'});self.assertEqual(status,200);self.assertIn(b'id: 9\r\n',data);self.assertIn(b'id: 11\r\n',data);self.assertNotIn(b'id: 8\r\n',data);self.assertEqual(self.value('/snapshot/TALK-1')['covers_through'],8)
        self.assertEqual(self.request('/__control',{'stage':0})[0],403)
