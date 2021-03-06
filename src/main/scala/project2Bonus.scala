import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import scala.util.Random
import akka.actor.PoisonPill

case class nodevisited(currentnode: Int, selectedneighbor: Int, algo: String)
case class simulate()
case class removenode(currentnode: Int,algo: String)
case class Gossiping()
case class completed()
case class fail (node:Int)
case class Pushsumpair ( s:Double, w: Double)
object start extends App{

     if(args.length == 3) {
       var numofnodes = args(0).toInt
       var algo = args(1)
       var topo = args(2)
       val system = ActorSystem("Gossipsimulator")
       var firstactor = system.actorOf(Props(new LeadNode(numofnodes, algo, topo)),"LeadNode")
       firstactor ! simulate()
     }
     else
       println("Input in format <numofnodes> < algo> <topology>" )
  }


  class LeadNode(numofnodes:Int , algo: String , topo: String) extends Actor {

    
    val system = ActorSystem("Workers")
    val neighbours = new Array[Int](6)
    var neighbourcount = 1
    var n = 0
    var visitcount = 0
    var actualumnodes = 0
    var completednodes = 0 
    if( topo == "3D" || topo == "3Dimp") {
       n = math.ceil(math.cbrt(numofnodes)).toInt
      actualumnodes = n*n*n
    }
    else
       actualumnodes = numofnodes
    val activeNodes = new Array[Int](actualumnodes)
	println("\nactualnodes "+actualumnodes)
    for(i <-0 to actualumnodes -1)
      activeNodes(i) = 1
   val workers = new Array [ActorRef](actualumnodes)
    topo match{
      case "line" =>
        BuildLine()
      case "3D" =>

      	Build3D()
      case "3Dimp"=>
 	      Build3D()
      case "full" =>
        BuildFull()	
      case _=>

    }
    val starttime = System.currentTimeMillis()
    def receive = {
      case simulate() =>
        var startnode = 0
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
      case nodevisited(currentnode: Int, neighbor : Int, algo : String) =>
        visitcount = visitcount+1
        if( visitcount == numofnodes ) {
          println("************All nodes visited ***************\n")
          println("***************"+algo+"******************")
          val endtime = System.currentTimeMillis()
          println(" Total time taken "+(endtime-starttime))
	  context.system.shutdown()
	  
        }
      case removenode (nodetobestopped :Int, algo: String)=>
        println("\n removed node "+nodetobestopped)
        println("remove node \n")
	activeNodes(nodetobestopped)=0
      case fail(killednode: Int) =>
	println("This node  "+killednode+"  got failed")
	//self ! PoisonPill
      case _ =>	
	
    }
   def BuildLine(){
        for(currentnode:Int <- 0 to numofnodes-1){
	      workers(currentnode) = context.actorOf(Props(new Workers(currentnode,topo,
            workers,numofnodes,self,activeNodes,neighbours,0,0,0,0)))
        }
  }
   def Build3D(){
      var l=0
        var index: Int = 0
        var currentnode = 0

        for(i:Int <- 0 to n-1)
          for(j:Int <- 0 to n-1)
            for(k:Int <- 0 to n-1)
            {
	      currentnode = (i*n + j)*n + k
              if(currentnode < n*n*n) {
                workers(currentnode) = context.actorOf(Props(new Workers(currentnode, topo,
                  workers, numofnodes, self, activeNodes, neighbours, l, i, j, k)))
		// println("\nCreating Worker Currentnode"+ currentnode)	  
              }
            }
    }
 def BuildFull(){
 	for(currentnode:Int <- 0 to numofnodes-1)
	workers(currentnode) = context.actorOf(Props(new Workers(currentnode, topo,
                  workers, numofnodes, self, activeNodes, neighbours, 0, 0, 0, 0)))
	}
  }

/********************************************************************************************************************************************
***********************************************Class Worker**********************************************************************************
*Functions: *****************************Functions to Find Neighbour of given topology ******************************************************
*line_findneighbor : returns neighbour of current node in line topology
*ThreeD_findneighbor:returns neigbour for 3D and 3D imperfect topology check3dimp distinguishes both
*pingself() : Retries after fixed instance of time
*Class case : Gossiping() : Handles Gossip : Terminating condition visitgossip_max
	      Pushsumpair(): Handles Gossip : Terminating condition visitpushsum_max and killratio *********************************************************************************************************************************************
*********************************************************************************************************************************************/
class Workers(currentnode:Int,topo:String,workers:Array[ActorRef],numofnodes:Int,Lead:ActorRef,activeNodes:Array[Int],neighbours:Array[Int],l: Int, row: Int, col: Int, ht: Int) extends Actor {
  val visitgossip_max = 10
  val visitpushsum_max = 3
  val checkratio = math.pow(10, -10)
  val n = math.ceil(math.cbrt(numofnodes))
  var s =(row*n +col)*n+ht
  var w = 1
  var ratio = s / w
  var noofvisit = 0
  var completed = false
  var selectedneighbor:Int= 0
  val Killratio = math.pow(10, -10)
  var killflag = true
  val killjump = 5

  def line_findneighbor(currentnode: Int): Int = {
    if (currentnode == 0)
      return 1
    if (currentnode == numofnodes - 1)
      return numofnodes - 2
    else {
      var selectnext = Random.nextInt(2)
      if (selectnext == 0)
        return currentnode - 1
      else
        return currentnode + 1
    }
  }
/******************************calculates Neighbour 3D using (row *n+col)*n+height*****************************/
  def ThreeD_findneighbor(currentnode: Int, check3dimp: Boolean, numneigh: Int, k: Int, j: Int, i: Int): Int = {
    var l = 0
    val n = math.ceil(math.cbrt(numofnodes)).toInt
    var neigbourarray = new Array[Int](7)

    if (k > 0) {
      neigbourarray(l) = i + n * (j + n * (k - 1)) // index as [i][j][k-1]
      l = l + 1
    }
    if (k < n) {
      neigbourarray(l) = i + n * (j + n * k + 1)
      l = l + 1
    }
    if (j > 0) {
      neigbourarray(l) = i + n * (j - 1 + n * k)
      l = l + 1
    }
    if (j < n) {
      neigbourarray(l) = i + n * (j + 1 + n * k)
      l = l + 1
    }
    if (i > 0) {
      neigbourarray(l) = i - 1 + n * (j + n * k)
      l = l + 1
    }
    if (i < n) {
      neigbourarray(l) = i + 1 + n * (j + n * k)
      l = l + 1
    }
    if (check3dimp) {
      
     neigbourarray(l) = Random.nextInt(n * n* n) %(n*n*n)
  l = l + 1
    }

    var selectnext = Random.nextInt(l)
    return neigbourarray(selectnext)
  }

  def pingself(algo: String) {
    val system = akka.actor.ActorSystem("Gossipsimulator")
    import system.dispatcher

    val retry = scala.concurrent.duration.FiniteDuration(10, "milliseconds")
    if (algo == "gossip")
      context.system.scheduler.scheduleOnce(retry, self, Gossiping())
    else
      context.system.scheduler.scheduleOnce(retry, self, Pushsumpair(s, w))
  }
 def kill(selectedneighbor: Int) {
   if (selectedneighbor % killjump == 0) {
     activeNodes(selectedneighbor) = 0
     killflag = false
     Lead ! fail(selectedneighbor)
   }
 }
  def receive = {
    case Gossiping() =>{
      if (completed == false) {
        if (noofvisit == 0) {
          Lead ! nodevisited(currentnode, selectedneighbor,"Gossiping")
        }
	         if(killflag == true){
            topo match {
	           case "line" => 
		        selectedneighbor = line_findneighbor(currentnode)
 		      kill(selectedneighbor)
           case  "3D" =>
		      selectedneighbor = ThreeD_findneighbor(currentnode, false, l, row, col, ht)
		      kill(selectedneighbor)
           case  "3Dimp" =>
		      selectedneighbor = ThreeD_findneighbor(currentnode, false, l, row, col, ht)
		    kill(selectedneighbor)	
	       case "full" =>
		      selectedneighbor = Random.nextInt(numofnodes)
		      kill(selectedneighbor)		
          }
          }
         }
          if (noofvisit == visitgossip_max) {
          completed = true
          Lead ! removenode(currentnode,"Gossiping")
        }
        else
          topo match {
            case "line" =>
              if (!sender.equals(self))
                noofvisit = activeNodes(selectedneighbor) - 1
              selectedneighbor = line_findneighbor(currentnode)
              if (activeNodes(selectedneighbor) != 0 && activeNodes(selectedneighbor) <= visitgossip_max) {
                activeNodes(selectedneighbor) = activeNodes(selectedneighbor) + 1
                println("selectedneighbor " + selectedneighbor + ":	Visit Count " + (activeNodes(selectedneighbor) - 1))
                workers(selectedneighbor) ! Gossiping()
              }

              pingself("gossip")

            case "3D" =>

              selectedneighbor = ThreeD_findneighbor(currentnode, false, l, row, col, ht)
              val n = math.ceil(math.cbrt(numofnodes)).toInt
              val boundary = n * n * n
              if (selectedneighbor < boundary) {
                if (activeNodes(selectedneighbor) != 0 && activeNodes(selectedneighbor) <= visitgossip_max) {
            	  if (!sender.equals(self))
                	noofvisit = activeNodes(selectedneighbor) - 1
                  activeNodes(selectedneighbor) = activeNodes(selectedneighbor) + 1
                  println("selectedneighbor " + selectedneighbor + ":	Visit Count " + (activeNodes(selectedneighbor) - 1))
                  workers(selectedneighbor) ! Gossiping()
                }
              }

              pingself("gossip")
            case "3Dimp" =>

              selectedneighbor = ThreeD_findneighbor(currentnode, true, l, row, col, ht)
              val n = math.ceil(math.cbrt(numofnodes)).toInt
              val boundary = n * n * n
              if (selectedneighbor < boundary) {
                if (activeNodes(selectedneighbor) != 0 && activeNodes(selectedneighbor) <= visitgossip_max) {
                  if (!sender.equals(self))
                    noofvisit = activeNodes(selectedneighbor) - 1
                  activeNodes(selectedneighbor) = activeNodes(selectedneighbor) + 1
                  println("selectedneighbor " + selectedneighbor + ":	Visit Count " + (activeNodes(selectedneighbor) - 1))
                  workers(selectedneighbor) ! Gossiping()
                }
              }


              pingself("gossip")
            case "full" =>
              if (!sender.equals(self))
                noofvisit = activeNodes(selectedneighbor) - 1
              selectedneighbor = Random.nextInt(numofnodes)
              if (activeNodes(selectedneighbor) != 0 && activeNodes(selectedneighbor) <= visitgossip_max) {
                activeNodes(selectedneighbor) = activeNodes(selectedneighbor) + 1
                println("selectedneighbor " + selectedneighbor + ":	Visit Count " + (activeNodes(selectedneighbor) - 1))
                workers(selectedneighbor) ! Gossiping()
              }
              pingself("gossip")
          }
      }

    case Pushsumpair(s: Double, w: Double) =>{
      if (completed == false) {
        val n = math.ceil(math.cbrt(numofnodes)).toInt
        val boundary = n * n * n 	
        if (noofvisit == 0)
          Lead ! nodevisited(currentnode, selectedneighbor,"Pushsum")
        if (noofvisit == visitpushsum_max) {
          completed = true
          Lead ! removenode(currentnode,"Pushsum")
        }
        else
          topo match {
            case "line" => var ratio_prev = this.s / this.w
              var ratio_cur =( this.s +s )/ (this.w +w)
              selectedneighbor = line_findneighbor(currentnode)
              this.s = this.s / 2
              this.w = this.w / 2
           if (activeNodes(selectedneighbor) != 0 && activeNodes(selectedneighbor) <= visitpushsum_max) {
              if (math.abs(ratio_prev - ratio_cur) < Killratio && !sender.equals(self))
                noofvisit = activeNodes(selectedneighbor) - 1
              else
                noofvisit = 0
                activeNodes(selectedneighbor) = activeNodes(selectedneighbor) + 1
                println("selectedneighbor " + selectedneighbor + ":	Visit Count " + (activeNodes(selectedneighbor) - 1))
              workers(selectedneighbor) ! Pushsumpair(this.s, this.w)
	}
     	pingself("push")

            case "3D" =>
              var ratio_prev = this.s / this.w
              var ratio_cur = (this.s+s) /(this.w+w )
              selectedneighbor = ThreeD_findneighbor(currentnode, false, l, row, col, ht)
              this.s = this.s / 2
              this.w = this.w / 2
           if (selectedneighbor < boundary && activeNodes(selectedneighbor) != 0 && activeNodes(selectedneighbor) <= visitpushsum_max) {
              if (math.abs(ratio_prev - ratio_cur) < Killratio && !sender.equals(self))
                noofvisit = activeNodes(selectedneighbor) - 1
              else
                noofvisit = 0
                activeNodes(selectedneighbor) = activeNodes(selectedneighbor) + 1
                println("selectedneighbor " + selectedneighbor + ":	Visit Count " + (activeNodes(selectedneighbor) - 1))
              workers(selectedneighbor) ! Pushsumpair(this.s, this.w)
	   }
 	pingself("push")
            case "3Dimp" =>
              var ratio_prev = this.s / this.w
              var ratio_cur = (this.s+s) / (this.w+w)
              selectedneighbor = ThreeD_findneighbor(currentnode, true, l, row, col, ht)
              this.s = this.s/2
              this.w = this.w/2
           if (selectedneighbor < boundary && activeNodes(selectedneighbor) != 0 && activeNodes(selectedneighbor) <= visitpushsum_max) {
            println("selectedneighbor " + selectedneighbor + ":	Visit Count " + (activeNodes(selectedneighbor) - 1))
              if (math.abs(ratio_prev - ratio_cur) < Killratio && !sender.equals(self))
                noofvisit = activeNodes(selectedneighbor) - 1
              else
                noofvisit = 0
                activeNodes(selectedneighbor) = activeNodes(selectedneighbor) + 1    
              workers(selectedneighbor) ! Pushsumpair(this.s, this.w)
	   }
       pingself("push")
           case "full" =>
              var ratio_prev = this.s / this.w
              var ratio_cur = (this.s+s) /(this.w+w)
              selectedneighbor = Random.nextInt(numofnodes)
              this.s = this.s / 2
              this.w = this.w / 2
           if (selectedneighbor < boundary && activeNodes(selectedneighbor) != 0 && activeNodes(selectedneighbor) <= visitpushsum_max) {
              if (math.abs(ratio_prev - ratio_cur) < Killratio && !sender.equals(self))
                noofvisit = activeNodes(selectedneighbor) - 1
              else
                noofvisit = 0
                activeNodes(selectedneighbor) = activeNodes(selectedneighbor) + 1
                println("selectedneighbor " + selectedneighbor + ":	Visit Count " + (activeNodes(selectedneighbor) - 1))
              workers(selectedneighbor) ! Pushsumpair(this.s, this.w)
		        }
          pingself("push")	
            case _ =>
          }
        }
  	}
  }

}

