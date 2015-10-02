import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import scala.util.Random


case class nodevisited(currentnode: Int)
case class simulate()
case class removenode(currentnode: Int)
case class Gossiping()
case class completed()
case class Pushsumpair ( s:Int, w: Int)
object start extends App{

     if(args.length == 3) {
       var numofnodes = args(0).toInt
       var algo = args(1)
       var topo = args(2)
       val system = ActorSystem("Gossipsimulator")
       if(topo =="3D" || topo =="3D imperfect")
       {
         var temp = math.floor(math.cbrt(numofnodes))
         numofnodes=math.pow(temp,3).toInt

       }
       var firstactor = system.actorOf(Props(new LeadNode(numofnodes, algo, topo)),"LeadNode")
       firstactor ! simulate()
     }
     else
       println("Input in format <numofnodes> < algo> <topology>" )
  }


class LeadNode(numofnodes:Int , algo: String , topo: String) extends Actor {

     val workers = new Array[ActorRef] (numofnodes)
     val system = ActorSystem("Workers")
     val neighbours = new Array[Int](2) // For linear case
     var neighbourcount = 1
     var visitcount = 0
     var completednodes = 0 // nodes that exceed maximum hopcount
     var activeNodes = new Array[Int](numofnodes)
    println("Leadnodes numofnodes \n"+numofnodes)
     for(i <-0 to numofnodes -1)
       activeNodes(i) = 1//Initially all alive


       topo match{
         case "line" =>
           println("\n line noofnodes"+numofnodes)
           for(currentnode:Int <- 0 to numofnodes-1){
             if(currentnode ==0) neighbours(0)=1
             else if(currentnode == numofnodes-1)
	 neighbours(0) = numofnodes-2
             else {
               neighbourcount = 2
               neighbours(0) = currentnode - 1
               neighbours(1) = currentnode + 1
             }
             workers(currentnode) = system.actorOf(Props(new Workers(currentnode,topo,
                                                    workers,numofnodes,self,activeNodes,neighbours)))

           }
         case "3D" =>

         case "Imperfect 3D"=>
         case "Full" =>
         case _=>

      }
      val starttime = System.currentTimeMillis()




  def receive = {
    case simulate() =>
        var startnode = scala.util.Random.nextInt(numofnodes) //master sends msg to random node
        println("\nneighbour of random node"+neighbours(0))
        println("Simulate start\n")
        if(algo.equalsIgnoreCase ("gossip"))
{
               println("Simulate Gossiping start\n")
               workers(startnode) ! Gossiping()
}
        else if(algo.equalsIgnoreCase ("Pushsum"))
             workers(startnode) ! Pushsumpair(startnode ,1)
        else
          println("Incorrect algorithm \n")

    case completed() =>
    case nodevisited(currentnode: Int) =>
         visitcount = visitcount+1
         println("\n node visited  current node"+visitcount + currentnode)
         if( visitcount == numofnodes ) {
           println("All nodes visited \n")
           val endtime = System.currentTimeMillis()
           println(" Total time taken "+(endtime-starttime))
         }
    case removenode (currentnode :Int)=>
        println("\n removed node "+currentnode)
 	println("remove node \n")
         workers.drop(currentnode)

          completednodes = completednodes +1
      if( completednodes == numofnodes) {
        println("All nodes completed ")
        System.exit(1)
      }

    case _ =>

    }

  }


class Workers(currentnode:Int,topo:String,workers:Array[ActorRef],numofnodes:Int,Lead:ActorRef,activeNodes:Array[Int],neighbours:Array[Int]) extends Actor
{
    val visitgossip_max = 10
    val visitpushsum_max = 3
    val checkratio = math.pow(10, -10)
    var s = currentnode
    var w = 1
    var ratio = s/w
    var noofvisit = 0 // no of times a node is visited
    var completed = false // set true when hop count is exceeded
    var selectedneighbor = 0
 
def receive ={
  case Gossiping() =>
      if(completed == false){
  
        if(noofvisit ==  0)
            Lead ! nodevisited(currentnode)
        if(noofvisit == visitgossip_max){
          completed = true
          Lead ! removenode(currentnode)
        }
             topo match{
          case "line" =>
             if(!sender.equals(self))
               noofvisit = noofvisit + 1
             if( currentnode == 0) 
              selectedneighbor = 1
             else if( currentnode == numofnodes - 1)
		selectedneighbor = numofnodes - 2
            else {
		 var selectnext = Random.nextInt(2)
		 if(selectnext == 0)
 		   selectedneighbor = currentnode - 1
 		else
		   selectedneighbor = currentnode + 1
	     }
            if(activeNodes(selectedneighbor) != 0){
	               workers(selectedneighbor ) ! Gossiping()
	 }

            }
             /* if(activeNodes(selectedneighbor))
              workers( selectedneighbor ) ! Gossiping
            }
            if( currentnode == numofnodes -1 ) {
              selectedneighbor = numofnodes - 2
              if(activeNodes(selectedneighbor))
              workers(selectedneighbor) ! Gossiping
            }*/
             /* else {
                   var selectnext = Random.nextInt(2)
 		println("\n Gossiping currentnode\tsnei\tactnode)"+ currentnode+  selectedneighbor + activeNodes(selectedneighbor))
                selectedneighbor = neighbours(selectnext)
              if(activeNodes(selectedneighbor) != 0)
                workers(selectedneighbor) ! Gossiping()
            }*/
               //println("\n Gossiping currentnode\tsnei\tactnode)"+ currentnode+  selectedneighbor + activeNodes(selectedneighbor))
        }
    

  case Pushsumpair(s :Int, w:Int)=>

  case _ =>
}


}
