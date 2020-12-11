package org.jlab.jnp.grapes.services;

import org.jlab.jnp.physics.LorentzVector;

import org.jlab.jnp.hipo4.data.Bank;
import org.jlab.jnp.hipo4.data.Event;
import org.jlab.jnp.hipo4.data.SchemaFactory;
import org.jlab.jnp.pdg.PDGDatabase;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.IntStream;
import java.util.Arrays;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;


/**
 * 
 * DV1PipPimP Skimming
 *
 * @author kenjo
 */

public class DV1PipPimPWagon extends BeamTargetWagon {

	static final double PionMass   = 0.13957f;

	public DV1PipPimPWagon() {
		super("DV1PipPimPWagon","kenjo","0.0");
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

		if( recPart!=null && recPart.getRows()>3 ){

            // electron selection
            List<Integer> eles = new ArrayList<>();
            Supplier<Boolean> goodEles = () -> eles.addAll(IntStream.range(0, recPart.getRows())
              .filter(ii -> recPart.getInt("pid", ii)==11)
              .filter(ii -> {
                int stat = Math.abs(recPart.getShort("status",ii));
                return stat>2000 && stat<4000;
              }).filter(ii -> {
                LorentzVector vv = getLorentzVectorWithPID(recPart, ii, 11);
                return vv.e()>0.1*beamEnergy;
              }).boxed().collect(Collectors.toList()));

            // proton selection
            List<Integer> pros = new ArrayList<>();
            Supplier<Boolean> goodPros = () -> pros.addAll(IntStream.range(0, recPart.getRows())
              .filter(ii -> recPart.getInt("pid", ii)==2212)
              .filter(ii -> {
                int stat = recPart.getShort("status",ii);
                return stat>2000;
              }).filter(ii -> {
                LorentzVector vv = getLorentzVectorWithPID(recPart, ii, 2212);
                return vv.e()>0.94358;
              }).boxed().collect(Collectors.toList()));

            // pi+ selection
            List<Integer> pips = new ArrayList<>();
            Supplier<Boolean> goodPips = () -> pips.addAll(IntStream.range(0, recPart.getRows())
              .filter(ii -> recPart.getInt("pid", ii)==211)
              .filter(ii -> {
                int stat = recPart.getShort("status",ii);
                return stat>2000 && stat<4000;
              }).filter(ii -> {
                LorentzVector vv = getLorentzVectorWithPID(recPart, ii, 211);
                return vv.e()>0.3;
              }).boxed().collect(Collectors.toList()));

            // pi- selection
            List<Integer> pims = new ArrayList<>();
            Supplier<Boolean> goodPims = () -> pims.addAll(IntStream.range(0, recPart.getRows())
              .filter(ii -> recPart.getInt("pid", ii)==-211)
              .filter(ii -> {
                int stat = recPart.getShort("status",ii);
                return stat>2000 && stat<4000;
              }).filter(ii -> {
                LorentzVector vv = getLorentzVectorWithPID(recPart, ii, -211);
                return vv.e()>0.3;
              }).boxed().collect(Collectors.toList()));

            // DIS && exclusivity cuts
            return goodEles.get() && goodPros.get() && goodPips.get() && goodPims.get()
              && eles.stream().filter(iele -> {
                  LorentzVector ele = getLorentzVectorWithPID(recPart, iele, 11);
                  LorentzVector vqq = new LorentzVector(beam).sub(ele);
                  LorentzVector vww = new LorentzVector(vqq).add(targ);

                  return -vqq.mass2()>0.8 && vww.mass()>1.8;
                }).flatMap(iele -> pros.stream().map(ipro -> Arrays.asList(iele,ipro)))
                .flatMap(ivvs -> pips.stream().map(ipip -> Arrays.asList(ivvs.get(0),ivvs.get(1),ipip)))
                .flatMap(ivvs -> pims.stream().map(ipim -> Arrays.asList(ivvs.get(0),ivvs.get(1),ivvs.get(2),ipim)))
                .anyMatch(ivvs -> {
                  LorentzVector ele = getLorentzVectorWithPID(recPart, ivvs.get(0), 11);
                  LorentzVector pro = getLorentzVectorWithPID(recPart, ivvs.get(1), 2212);
                  LorentzVector pip = getLorentzVectorWithPID(recPart, ivvs.get(2), 211);
                  LorentzVector pim = getLorentzVectorWithPID(recPart, ivvs.get(3), -211);

                  LorentzVector vww = new LorentzVector(beam).sub(ele).add(targ);

                  LorentzVector vmisspro = new LorentzVector(vww).sub(pip).sub(pim);
                  LorentzVector vmisspip = new LorentzVector(vww).sub(pro).sub(pim);
                  LorentzVector vmisspim = new LorentzVector(vww).sub(pro).sub(pip);
                  LorentzVector vmiss0 = new LorentzVector(vmisspro).sub(pro);

                  return Math.abs(vmiss0.e())<0.5
                      && Math.abs(vmiss0.mass2())<0.1
                      && vmiss0.px()*vmiss0.px() + vmiss0.py()*vmiss0.py() < 0.5
                      && Math.toDegrees(pro.angle(vmisspro)) < 12
                      && Math.toDegrees(pip.angle(vmisspip)) < 12
                      && Math.toDegrees(pim.angle(vmisspim)) < 12
                      && vmisspro.mass2() > 0.5 && vmisspro.mass2() < 1.5
                      && vmisspip.mass2() > -0.3 && vmisspip.mass2() < 0.5
                      && vmisspim.mass2() > -0.3 && vmisspim.mass2() < 0.5;
               });
          }

          return false;
     }
}
