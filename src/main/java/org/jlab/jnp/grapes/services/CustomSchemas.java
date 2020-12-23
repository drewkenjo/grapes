/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jlab.jnp.grapes.services;

import org.jlab.jnp.hipo4.data.Schema;
import org.jlab.jnp.hipo4.data.Schema.SchemaBuilder;

/**
 *
 * @author kenjo
 */
public abstract class CustomSchemas {
    public static Schema[] getCustomSchemas() {
      SchemaBuilder excl = new SchemaBuilder("EXCLUSIVE::ePipPimP", 555,5);
      excl.addEntry("ex","F","ex");
      excl.addEntry("ey","F","ey");
      excl.addEntry("ez","F","ez");
      excl.addEntry("pipx","F","pipx");
      excl.addEntry("pipy","F","pipy");
      excl.addEntry("pipz","F","pipz");
      excl.addEntry("pimx","F","pimx");
      excl.addEntry("pimy","F","pimy");
      excl.addEntry("pimz","F","pimz");
      excl.addEntry("prox","F","prox");
      excl.addEntry("proy","F","proy");
      excl.addEntry("proz","F","proz");

      return new Schema[]{excl.build()};
    }
}
