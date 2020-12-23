#!/usr/bin/env run-groovy
import org.jlab.jnp.hipo4.io.HipoReader
import org.jlab.jnp.hipo4.data.Bank
import org.jlab.jnp.hipo4.data.Event
import org.jlab.jnp.hipo4.data.SchemaFactory
import org.jlab.jroot.ROOTFile


def reader = new HipoReader()
reader.open(args[0])
def event = new Event()
def schema = reader.getSchemaFactory().getSchema(args[1])
def bank = new Bank(schema)
def varnames = schema.getEntryList()

def ff = new ROOTFile(args[0]+'.root')
def tt = ff.makeNtuple('h22','title',varnames.join(":"))


while(reader.hasNext()) {
  reader.nextEvent(event)
  event.read(bank)

  if(bank && bank.getRows()>0) {
    (0..<bank.getRows()).each{irow->
      tt.fill(*varnames.collect{bank.getFloat(it,irow)})
    }
  }
}

tt.write()
ff.close()
