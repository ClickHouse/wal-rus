#!/usr/bin/awk -f
# Merge lcov tracefiles by summing per-line and per-function hit counts
# Function merge best-effort by mangled name since inventories differ per object
/^SF:/   { sf=substr($0,4); if(!(sf in seen)){seen[sf]; sford[++ns]=sf} next }
/^DA:/   { i=index(c=substr($0,4),","); ln=substr(c,1,i-1)
           if(!((sf,ln) in da)) lord[sf,++nl[sf]]=ln
           da[sf,ln]+=substr(c,i+1); next }
/^FN:/   { i=index(c=substr($0,4),","); nm=substr(c,i+1)
           if(!((sf,nm) in fnln)){fnln[sf,nm]=substr(c,1,i-1); fnord[sf,++nf[sf]]=nm} next }
/^FNDA:/ { i=index(c=substr($0,6),","); fnda[sf,substr(c,i+1)]+=substr(c,1,i-1); next }
END{
 for(s=1;s<=ns;s++){
  f=sford[s]; print "SF:" f; m=nf[f]+0; fh=0
  for(i=1;i<=m;i++){nm=fnord[f,i]; print "FN:" fnln[f,nm] "," nm}
  for(i=1;i<=m;i++){nm=fnord[f,i]; h=((f,nm) in fnda)?fnda[f,nm]:0
                    print "FNDA:" h "," nm; if(h>0)fh++}
  print "FNF:" m; print "FNH:" fh
  c=nl[f]+0; for(i=1;i<=c;i++)k[i]=lord[f,i]
  for(i=2;i<=c;i++){x=k[i];j=i-1;while(j&&k[j]+0>x+0){k[j+1]=k[j];j--}k[j+1]=x}
  lh=0; for(i=1;i<=c;i++){v=da[f,k[i]]; print "DA:" k[i] "," v; if(v>0)lh++}
  print "LF:" c; print "LH:" lh; print "end_of_record"
 }
}
