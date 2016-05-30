##### Must be casa 4.6 #####
ver=casa["build"]["version"]
if (float(ver[0:3])<4.6):
    print "CASA VERSION TOO LOW: "+ver

# list of files = days
vl=["20131025_951.ms","20131031_951.ms","20131109_951.ms","20131112_951.ms","20131116_946.ms","20131118_946.ms","20131119_941.ms","20131121_946.ms","20131122_941.ms","20131123_951.ms","20131126_946.ms","20131203_941_6_FINAL_PRODUCTS.ms","20131203_941.ms","20131206_951.ms","20131210_951.ms","20131212_946.ms","20131213_941.ms","20131217_951.ms","20131218_946.ms","20131220_941.ms","20131221_951.ms","20131222_941.ms","20131226_946.ms","20131227_951.ms","20131229_946.ms","20131230_941.ms","20131231_941.ms","20140102_941.ms","20140105_941.ms","20140105_946.ms","20140106_946.ms","20140108_951.ms","20140112_951.ms","20140115_941.ms","20140116_941.ms","20140117_946.ms","20140118_941.ms","20140119_941.ms","20140119_946.ms","20140120_941.ms","20140121_951.ms","20131117_941.ms"]


# run through the days
for nm in range(2,3):#0,len(vl)):
    print 'Starting on '+vl[nm]
    vis='Averaged_Vis/'+vl[nm]
    tb.open(vis,nomodify=F)
    #tb.removecols("CORRECTED_DATA")
    tb.close()
    #clearcal(vis=vis,addmodel=T)
    #delmod(vis=vis,otf=T,scr=T)
    im.open(thems=vis,usescratch=T)
    case=False
    for ns in range(0,15):
        spw=str(ns)
        print 'Starting on SPW'+spw
        #im.selectvis(vis='Averaged_Vis/'+vl[nm],spw=str(ns));
        im.selectvis(spw=str(ns));
        im.defineimage(nx=2048,ny=2048,cellx='1.5arcsec',celly='1.5arcsec',mode='mfs',facets=1)
        im.setoptions(ftmachine='wproject',wprojplanes=12)
        mdl=['Epoch1_Images_Wproject/epoch1.mmstest_spw_'+spw+'.model.tt0','Epoch1_Images_Wproject/epoch1.mmstest_spw_'+spw+'.model.tt1']
        ntt=len(mdl) ## Reset to something shorter if wished
        ms.open(vis)
        fq=ms.getspectralwindowinfo()
        fq=fq[spw]['RefFreq']
        ms.close()
        im.settaylorterms(ntaylorterms=ntt,reffreq=fq)
        #im.settaylorterms(ntaylorterms=2)
        #im.ft(model=['Epoch1_Images_Wproject/epoch1.mmstest_spw_'+spw+'.model.tt0','Epoch1_Images_Wproject/epoch1.mmstest_spw_'+spw+'.model.tt1'],incremental=False)
        im.ft(model=mdl[0:ntt],incremental=False)
        #im.clean(algorithm='wfclark',niter=10,image='test_image',residual='test_resid',model=['Epoch1_Images_Wproject/epoch1.bb.gt4k.si_spw_'+spw+'.model.tt0','Epoch1_Images_Wproject/epoch1.bb.gt4k.si_spw_'+spw+'.model.tt1'])
        case=True
    im.close()
    uvsub(vis=vis,reverse=False)
    print 'Finished with '+vl[nm]

#End
