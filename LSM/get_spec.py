import glob
import numpy as np
pxsa_a=[]
pxms_a=[]
fail_list=[]

#on='2'
flx_cut=[0,0.1e-3,0.1e-3,0.1e-3,0.1e-3,0.1e-3,0.1e-3]
# Make file list and ia stats
for n in range(15):
    if (n==0):
        fll=[]                                                         
        fl=[]
        ias=[]
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

tmp=np.zeros((len(ias),5))
for n in range(len(ias)):
    tmp[n]=[ias[n][0],ias[n][1],ias[n][2]['refval'][3],ias[n][2]['incr'][3],ias[n][2]['shape'][3]]

Ix=np.where(tmp.T[4]>25)[0]
len(Ix)

pxsa=[]

for ha in range(18):
  px=[]
  ll=[]
  pxm=[]
  tmp=ias[ha:270:18]
  fq=np.zeros((15,21))
  for n in range(15):
    for m in range(21):
        fq[n][m]=tmp[n][2]['refval'][3]+float(m)*tmp[n][2]['incr'][3]
  #
  # get the pixel values
  for f in fl[ha:270:18]:
    ia.open(f)     
    ll.append(f)
    px.append(ia.getregion(region='Outliers/Outlier_'+on+'.crtf'))
    pxm.append(ia.getregion(region='Outliers/Outlier_'+on+'.crtf',getmask=True))
    ia.close()
  ny=px[0].shape
  ns=ny[3];nx=ny[0];ny=ny[1];
  nvalid=len(np.where(pxm[0].T[0][0])[0])
  pxs=np.zeros((nvalid,15*21))
  for k in range(15):
    i=-1
    for n in range(nx):
        for m in range(ny):
           if pxm[0][n][m][0][0]:
            i=i+1;
            for j in range(ns):
             pxs[i][k*ns+j]=px[k][n][m][0][j]
  pxsa.append(pxs)

pxsa=np.array(pxsa)

# 315 = 15*21
# 245 = 14*16 +21
pxs_new=np.ones((18,nvalid,245))*np.nan
fq_new=np.zeros(245)
pxms_new=np.ones((18,nvalid),dtype='bool')

for m in range(18):
 for n  in range(nvalid):
    for i in range(15):
        if (i==14):
            for j in range(2,21):
                pxs_new[m][n][i*16+j]=pxsa[m][n][i*21+j]
        elif (i>0):
            for j in range(2,18):
                pxs_new[m][n][i*16+j]=pxsa[m][n][i*21+j]
        else:
            for j in range(18):
                pxs_new[m][n][j]=pxsa[m][n][j]


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
    pl.clf()
    pl.plot(fq_new,pxs_new[n].T)
    pl.title('HA: '+str(n-10))
    pl.savefig('model_Outlier_%s_HA_%02d.png'%(on,n))

for m in range(18):
    Ix=np.where(np.mean(np.abs(pxs_new[m]),axis=1)<flx_cut[int(on)])[0]
    for k in range(len(Ix)):
        pxms_new[m][Ix[k]]=False
    mn=np.mean(np.std(np.diff(pxs_new[m]),axis=0))
    Ix=np.where((np.std(np.diff(pxs_new[m]),axis=0)>3*mn)|
                (np.std(np.diff(pxs_new[m]),axis=0)==0))[0]+1
    Inx=np.where(((np.std(np.diff(pxs_new[m]),axis=0)<=3*mn)&
                (np.std(np.diff(pxs_new[m]),axis=0)!=0)))[0]+1
    for n in range(nvalid):
        if (pxms_new[m][n]):
            #for k in range(len(Ix)):
            #    pxs_new[m][n][Ix[k]]=np.interp(Ix[k],Inx,pxs_new[m][n][Inx])
            pxs_new[m][n][Ix]=np.interp(Ix,Inx,pxs_new[m][n][Inx])
            mnn=np.mean(np.abs(np.diff(pxs_new[m]))[n])
            Ixp=np.where(np.abs(np.diff(pxs_new[m]))[n]>3*mnn)[0]+1
            Inxp=np.where(np.abs(np.diff(pxs_new[m]))[n]<=3*mnn)[0]+1
            pxs_new[m][n][Ixp]=np.interp(Ixp,Inxp,pxs_new[m][n][Inxp])
        else:
            pxs_new[m][n][:]=0

pxsa_a.append(pxs_new)
pxms_a.append(pxms_new)

for n in range(18):
    pl.figure(n)
    pl.clf()
    pl.plot(fq_new,pxs_new[n].T)
    pl.title('HA: '+str(n-10))
    pl.savefig('new_model_Outlier_%s_HA_%02d.png'%(on,n))

for m in range(18):
  for n in range(15):
   f='Outliers/HA_'+str(m-10)+'/Outlier_'+on+'.0,8.spw_'+str(n)+'.model'
   os.system('rm -r '+f+'.new')
   try: 
       ia.open(f)
       im_new=ia.subimage(outfile=f+'.new',region='Outliers/Outlier_'+on+'.crtf')
       ia.close()
       px=im_new.getregion() #region='Outliers/Outlier_'+on+'.crtf')
       pxm=im_new.getregion(getmask=True) #,region='Outliers/Outlier_'+on+'.crtf')
       ny=px.shape
       ns=ny[3];nx=ny[0];ny=ny[1];
       npx=-1
       for i in range(nx):
           for j in range(ny):
               npx=npx+1
               for k in range(ns):
                   px[i][j][0][k]=pxs_new[m][npx][k+n*16]
                   pxm[i][j][0][k]=pxms_new[m][npx]
       #
       im_new.putregion(pixels=px,pixelmask=pxm)
       im_new.close()
   except:
        fail_list.append(f)
