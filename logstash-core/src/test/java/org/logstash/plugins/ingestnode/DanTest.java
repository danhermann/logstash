package org.logstash.plugins.ingestnode;

import java.util.HashMap;
import java.util.Map;

public class DanTest {

    public static void main(String[] args) {
        Map<String, String> ctx = new HashMap<>();
        ctx.put("message", "a=b,c=d,e=f,g=h");

        String s = ctx.get("message");
        int idx = s.indexOf(',');
        int last = 0;
        while (idx > -1) {
            String kv = s.substring(last, idx);
            int eqidx = kv.indexOf('=');
            ctx.put(kv.substring(0,eqidx), kv.substring(eqidx+1));
            last = idx + 1;
            idx = s.indexOf(',', last);

        }
        String kv = s.substring(last);
        int eqidx = kv.indexOf('=');
        ctx.put(kv.substring(0,eqidx), kv.substring(eqidx+1));
        //ctx.put("custom_", Integer.toString(keys.size()));

    }
}


//          "source": "ArrayList keys = new ArrayList(); String s = ctx.message; for (int k=0; k<s.length(); k++) { keys.add(s.substring(k));  } ctx.newField = Integer.toString(keys.size())"

/*


String s = ctx.message; int idx = s.indexOf(','); int last = 0; while (idx > -1) { String kv = s.substring(last, idx); int eqidx = kv.indexOf('='); ctx[kv.substring(0,eqidx)] = kv.substring(eqidx+1); last = idx + 1; idx = s.indexOf(',', last); } String kv = s.substring(last); int eqidx = kv.indexOf('='); ctx[kv.substring(0,eqidx)] = kv.substring(eqidx+1);



 */