package org.jlab.jnp.grapes.services;

import org.jlab.jnp.physics.LorentzVector;

import org.jlab.jnp.hipo4.data.Bank;
import org.jlab.jnp.hipo4.data.Event;
import org.jlab.jnp.hipo4.data.SchemaFactory;
import org.jlab.jnp.pdg.PDGDatabase;

import org.jlab.groot.data.H1F;

import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.IntStream;
import java.util.Arrays;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;



/**
 * 
 * DVPi0PWagon Skimming
 *
 * @author kenjo
 */

public class DVPi0PWagon extends BeamTargetWagon {

    static final double PionMass   = 0.13957f;

    private MsgClient red;
    private MsgClient.Stage stg1, stg2, stg3, stg4;

    public DVPi0PWagon() {
       super("DVPi0PWagon","kenjo","0.0");
       red = new MsgClient("kenjo", "testflow")
         .stages("stage1", "stage2", "stage3", "stage4");


       stg1 = red.stage("stage1").register(new H1F("hggm", "", 100,0.05,0.2));
       stg2 = red.stage("stage2").register(new H1F("hggm", "", 100,0.05,0.2));
       stg3 = red.stage("stage3").register(new H1F("hggm", "", 100,0.05,0.2));
       stg4 = red.stage("stage4").register(new H1F("hggm", "", 100,0.05,0.2));

       red.enable();
    }


    public static class Index {
       int index, sector, det;
       Boolean valid = true;
       public Index(int index) {
         this.index = index;
       }

       public Index setSector(OptionalInt osector) {
         if(osector.isPresent()) {
           sector = osector.getAsInt();
         } else {
           valid = false;
         }
         return this;
       }
    }


    private static LorentzVector getLorentzVectorWithPID(Bank bnk, int ii, int pid) {
       float px = bnk.getFloat("px", ii);
       float py = bnk.getFloat("py", ii);
       float pz = bnk.getFloat("pz", ii);
       LorentzVector lvec = new LorentzVector();
       double mass = PDGDatabase.getParticleById(pid).mass();
       lvec.setPxPyPzM(px,py,pz,mass);
       return lvec;
    }

    @Override
    public boolean processDataEvent(Event event, SchemaFactory factory) {
        LorentzVector beam = new LorentzVector(0,0,beamEnergy,beamEnergy);
        LorentzVector targ = new LorentzVector(0,0,0,targetMass);

        Bank recPart = new Bank(factory.getSchema("REC::Particle"));
        event.read(recPart);
        Bank ecBank = new Bank(factory.getSchema("REC::Calorimeter"));
        event.read(ecBank);

        if( recPart!=null && recPart.getRows()>3 && ecBank!=null ){

            List<Index> eles = new ArrayList<>();
            List<Index> pros = new ArrayList<>();
            List<Index> gms = new ArrayList<>();

            
            Supplier<Boolean> goodEles = () -> eles.addAll(IntStream.range(0, recPart.getRows())
              .filter(iele -> recPart.getInt("pid", iele)==11 && recPart.getShort("status",iele)<0).boxed()
              .map(iele -> new Index(iele))
              .map(ele -> ele.setSector(IntStream.range(0, ecBank.getRows()).filter(it -> ecBank.getShort("pindex",it)==ele.index && ecBank.getByte("detector",it)==7).findFirst()))
              .filter(ele -> ele.valid).collect(Collectors.toList()));

            Supplier<Boolean> goodPros = () -> pros.addAll(IntStream.range(0, recPart.getRows())
              .filter(ipro -> recPart.getInt("pid", ipro)==2212).boxed()
              .map(ipro -> new Index(ipro))
              .filter(pro -> pro.valid).collect(Collectors.toList()));

            Supplier<Boolean> goodGammas = () -> gms.addAll(IntStream.range(0, recPart.getRows())
              .filter(igm -> recPart.getInt("pid", igm)==22).boxed()
              .map(igm -> new Index(igm))
              .map(gm -> gm.setSector(IntStream.range(0, ecBank.getRows()).filter(it -> ecBank.getShort("pindex",it)==gm.index && ecBank.getByte("detector",it)==7).findFirst()))
              .filter(gm -> gm.valid).collect(Collectors.toList()));


            if(goodEles.get() && goodPros.get() && goodGammas.get() && gms.size()>2) {
              List<double[]> rows = eles.stream()
                .flatMap(ele -> pros.stream().map(pro -> Arrays.asList(ele, pro)))
                .flatMap(inds -> gms.stream().map(gm -> Arrays.asList(inds.get(0), inds.get(1), gm)))
                .flatMap(inds -> gms.stream().map(gm -> Arrays.asList(inds.get(0), inds.get(1), inds.get(2), gm)))
                .filter(inds -> inds.get(3).index > inds.get(2).index)
                .map(inds -> {
                  int iele = inds.get(0).index, ipro = inds.get(1).index, ig1 = inds.get(2).index, ig2 = inds.get(3).index;

                  LorentzVector ele = getLorentzVectorWithPID(recPart, iele, 11);
                  LorentzVector pro = getLorentzVectorWithPID(recPart, ipro, 2212);
                  LorentzVector gm1 = getLorentzVectorWithPID(recPart, ig1, 22);
                  LorentzVector gm2 = getLorentzVectorWithPID(recPart, ig2, 22);

                  LorentzVector vqq = new LorentzVector(beam).sub(ele);
                  LorentzVector vww = new LorentzVector(beam).sub(ele).add(targ);

                  LorentzVector vgg = new LorentzVector(gm1).add(gm2);

                  int pdet = recPart.getShort("status",ipro)/2000;
                  int esec = inds.get(0).sector;
                  int g1sec = inds.get(2).sector;
                  int g2sec = inds.get(3).sector;

                  int g1ecn = IntStream.range(0, ecBank.getRows()).filter(it -> ecBank.getShort("pindex",it)==inds.get(2).index && ecBank.getByte("detector",it)==7).toArray().length;
                  int g2ecn = IntStream.range(0, ecBank.getRows()).filter(it -> ecBank.getShort("pindex",it)==inds.get(3).index && ecBank.getByte("detector",it)==7).toArray().length;

                  stg1.fill("hggm", vgg.mass());

                  if(esec!=g1sec && esec!=g2sec) {
                    stg2.fill("hggm", vgg.mass());

                    if(g1ecn>1 && g2ecn>1) {
                      stg3.fill("hggm", vgg.mass());

                      if(pdet==2) {
                        stg4.fill("hggm", vgg.mass());

                        return new double[]{ele.px(), ele.py(), ele.pz(),
                                       pro.px(), pro.py(), pro.pz(),
                                       gm1.px(), gm1.py(), gm1.pz(),
                                       gm2.px(), gm2.py(), gm2.pz(),
                                       esec, pdet, 0, 0
                                      };
                      }
                    }
                  }
                  return new double[]{};
                }).filter(arr -> arr.length>0).collect(Collectors.toList());


              if(rows.size()>0) {
                Bank excl = new Bank(factory.getSchema("EXCLUSIVE::ePi0P"), rows.size());
                for(int irow=0;irow<rows.size();irow++) {
                  String[] names = "ex:ey:ez:px:py:pz:g1x:g1y:g1z:g2x:g2y:g2z:esec:pdet:run:status".split(":");
                  for(int ivar=0;ivar<names.length;ivar++) {
                    excl.putFloat(names[ivar], irow, (float) rows.get(irow)[ivar]);
                  }
                }
                event.write(excl);
                return true;
              }
            }
          }

          return false;
     }

     @Override
     public void destroy() {
       red.close();
     }
}
