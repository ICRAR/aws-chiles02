import glob
import numpy as np

for n in range(15):
    if (n==0):
        fll=[]                                                         
        fl=[]
        ias=[]
        ian=[]
    for m in range (-10,8):
        fl.append(glob.glob('Outliers/HA_'+str(m)+'/Outlier_'+on+'.0,8.spw_'+str(n)+'.model')[0])
        if (len(fl[-1])==0):
            fll.append(fl[-1])
            print '%d %d failed on '+fl[-1]
            fl.remove(fl[-1])
        else:
            ia.open(fl[-1])
            ias.append([n,m,ia.summary()])
            ia.close()
            ia.open(fl[-1]+'.new')
            ian.append([n,m,ia.summary()])
            ia.close()

tmpo=np.zeros((len(ias),5))
tmpn=np.zeros((len(ias),5))
for n in range(len(ias)):
    tmpo[n]=[ias[n][0],ias[n][1],ias[n][2]['refval'][3],ias[n][2]['incr'][3],ias[n][2]['shape'][3]]
    tmpn[n]=[ian[n][0],ian[n][1],ian[n][2]['refval'][3],ian[n][2]['incr'][3],ian[n][2]['shape'][3]]

#tmpoa.append(tmpo)
#tmpna.append(tmpn)


pxsa=[]
pxsan=[]

for ha in range(18):
  px=[]
  pxm=[]
  pxn=[]
  pxmn=[]
  tmp=ias[ha:270:18]
  fq=np.zeros((15,21))
  for n in range(15):
    for m in range(21):
        fq[n][m]=tmp[n][2]['refval'][3]+float(m)*tmp[n][2]['incr'][3]
  #
  # get the pixel values
  for f in fl[ha:270:18]:
    ia.open(f)     
    px.append(ia.getregion(region='Outliers/Outlier_'+on+'.crtf'))
    pxm.append(ia.getregion(region='Outliers/Outlier_'+on+'.crtf',getmask=True))
    ia.close()
    ia.open(f+'.new')     
    pxn.append(ia.getregion(region='Outliers/Outlier_'+on+'.crtf'))
    pxmn.append(ia.getregion(region='Outliers/Outlier_'+on+'.crtf',getmask=True))
    ia.close()
  ny=px[0].shape
  ns=ny[3];nx=ny[0];ny=ny[1];
  nvalid=len(np.where(pxm[0].T[0][0])[0])
  pxs=np.zeros((nvalid,15*21))
  pxsn=np.zeros((nvalid,15*21))
  for k in range(15):
    i=-1
    for n in range(nx):
        for m in range(ny):
           if pxm[0][n][m][0][0]:
            i=i+1;
            for j in range(ns):
             pxs[i][k*ns+j]=px[k][n][m][0][j]
             pxsn[i][k*ns+j]=pxn[k][n][m][0][j]
  pxsa.append(pxs)
  pxsan.append(pxsn)

pxsa=np.array(pxsa)
pxsan=np.array(pxsan)

# 315 = 15*21
# 245 = 14*16 +21
pxs_new=np.ones((18,nvalid,245))*np.nan
pxsn_new=np.ones((18,nvalid,245))*np.nan
fq_new=np.zeros(245)
pxms_new=np.ones((18,nvalid),dtype='bool')

for m in range(18):
 for n  in range(nvalid):
    for i in range(15):
        if (i==14):
            for j in range(2,21):
                pxs_new[m][n][i*16+j]=pxsa[m][n][i*21+j]
                pxsn_new[m][n][i*16+j]=pxsan[m][n][i*21+j]
        elif (i>0):
            for j in range(2,18):
                pxs_new[m][n][i*16+j]=pxsa[m][n][i*21+j]
                pxsn_new[m][n][i*16+j]=pxsan[m][n][i*21+j]
        else:
            for j in range(18):
                pxs_new[m][n][j]=pxsa[m][n][j]
                pxsn_new[m][n][j]=pxsan[m][n][j]


for i in range(15):
        if (i==14):
            for j in range(2,21):
                fq_new[i*16+j]=fq[i][j]
        elif (i>0):
            for j in range(2,18):
                fq_new[i*16+j]=fq[i][j]
        else:
            for j in range(18):
                fq_new[j]=fq[i][j]

for n in range(18):
    pl.figure(n)
    pl.subplot(2,1,1)
    pl.plot(fq_new,pxs_new[n].T)
    pl.title('HA: '+str(n-10))
    pl.subplot(2,1,2)
    pl.plot(fq_new,pxsn_new[n].T)
    pl.title('HA: '+str(n-10))
    pl.savefig('all_model_Outlier_%s_HA_%02d.png'%(on,n))

