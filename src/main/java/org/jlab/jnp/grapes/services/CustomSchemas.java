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
    private static Schema buildCustomSchema(String name, int id1, int id2, String vars) {
      SchemaBuilder schm = new SchemaBuilder(name, id1, id2);
      for(String vname: vars.split(":")) {
        schm.addEntry(vname, "F", vname);
      }
      return schm.build();
    }


    public static Schema[] getCustomSchemas() {
      return new Schema[]{
        buildCustomSchema("EXCLUSIVE::ePipPimP", 555, 5, "ex:ey:ez:pipx:pipy:pipz:pimx:pimy:pimz:prox:proy:proz"),
        buildCustomSchema("EXCLUSIVE::ePi0P", 444, 4, "ex:ey:ez:px:py:pz:g1x:g1y:g1z:g2x:g2y:g2z:esec:pdet:run:status")
      };
    }
}
