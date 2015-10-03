import akka.actor.{Actor, ActorRef, ActorSystem, Props}

import scala.util.Random


case class nodevisited(currentnode: Int, selectedneighbor: Int)
case class simulate()
case class removenode(currentnode: Int)
case class Gossiping()
case class completed()
case class Pushsumpair ( s:Double, w: Double)
object start extends App{

     if(args.length == 3) {
       var numofnodes = args(0).toInt
       var algo = args(1)
       var topo = args(2)
       val system = ActorSystem("Gossipsimulator")
   /*    if(topo =="3D" || topo =="3D imperfect")
       {
         var temp = math.floor(math.cbrt(numofnodes))
         numofnodes=math.pow(temp,3).toInt

       }*/
       var firstactor = system.actorOf(Props(new LeadNode(numofnodes, algo, topo)),"LeadNode")
       firstactor ! simulate()
     }
     else
       println("Input in format <numofnodes> < algo> <topology>" )
  }


  class LeadNode(numofnodes:Int , algo: String , topo: String) extends Actor {

    
    val system = ActorSystem("Workers")
    val neighbours = new Array[Int](6)
 // For linear case
    var neighbourcount = 1
    var n = 0
    var visitcount = 0
    var actualumnodes = 0
    var completednodes = 0 // nodes that exceed maximum hopcount

//Initially all alive
    //var startrandomnode: ActorRef
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
        //startrandomnode=buildline(numofnodes)



     //   println("\n line noofnodes"+numofnodes)
        for(currentnode:Int <- 0 to numofnodes-1){
          if(currentnode ==0) neighbours(0)
          else if(currentnode == numofnodes-1)
            neighbours(0) = numofnodes-2
          else {
            neighbourcount = 2
            neighbours(0) = currentnode - 1
            neighbours(1) = currentnode + 1
          }
	workers(currentnode) = context.actorOf(Props(new Workers(currentnode,topo,
            workers,numofnodes,self,activeNodes,neighbours,0,0,0,0)))

        }
      case "3D" =>
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
      case "3Dimp"=>
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

      case "Full" =>
      case _=>

    }
    //def buildline(numofnodes: Int ): ActorRef
    val starttime = System.currentTimeMillis()




    def receive = {
      case simulate() =>
        var startnode = 0//scala.util.Random.nextInt(numofnodes) //master sends msg to random node
      //  println("Simulate start\n")
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
      case nodevisited(currentnode: Int, neighbor : Int) =>
        visitcount = visitcount+1
	//println("   visitcount"+ visitcount)

        if( visitcount == numofnodes ) {
          println("*****************All nodes visited \n")
          val endtime = System.currentTimeMillis()
          println(" Total time taken "+(endtime-starttime))
	  //System.exit(1)
	  
        }
      case removenode (nodetobestopped :Int)=>
        println("\n removed node "+nodetobestopped)
        println("remove node \n")
	activeNodes(nodetobestopped)=0
        

      case _ =>
	
	
    }

  }


class Workers(currentnode:Int,topo:String,workers:Array[ActorRef],numofnodes:Int,Lead:ActorRef,activeNodes:Array[Int],neighbours:Array[Int],l: Int, row: Int, col: Int, ht: Int) extends Actor
{
    val visitgossip_max = 20
    val visitpushsum_max = 3
    val checkratio = math.pow(10, -10)
    var s = currentnode
    var w = 1
    var ratio = s/w
    var noofvisit = 0 // no of times a node is visited
    var completed = false // set true when hop count is exceeded
    var selectedneighbor = 0
    var s_self =0.0
    var w_self =0.0
    val Killratio = math.pow(10,-9)
   // var ThreeDflag: Boolean
def receive = {
     case Gossiping() =>
       if (completed == false) {
	//println("currentnode " + currentnode +":Visit Count "+noofvisit)
         if (noofvisit == 0)
	{
           Lead ! nodevisited(currentnode, selectedneighbor)
	}
         if (noofvisit == visitgossip_max) {
           completed = true
		context.stop(self)
           Lead ! removenode(currentnode)
         }
         topo match {
           case "line" =>
             if (!sender.equals(self)) 
               noofvisit = noofvisit + 1
               selectedneighbor = line_findneighbor(currentnode)
               if (activeNodes(selectedneighbor) != 0) {
           //      println("\n currentnode "+currentnode +"selectedneighbor"+ selectedneighbor)
			  println("selectedneighbor"+ selectedneighbor + "Visit Count" + (activeNodes(selectedneighbor)-1))
		
                 workers(selectedneighbor) ! Gossiping()
               }
             	
		
	            // val retry = scala.concurrent.duration.FiniteDuration(30, "milliseconds")
	             // context.system.scheduler.scheduleOnce(retry, self, Gossiping())

           case "3D" =>
             if (!sender.equals(self)) 
               noofvisit = noofvisit + 1
               selectedneighbor = ThreeD_findneighbor(currentnode, false, l,row,col,ht)
               val n = math.ceil(math.cbrt(numofnodes)).toInt
               val boundary = n * n * n
		
		//println("\n Boundary " + boundary + " selectedneighbor " + selectedneighbor)
               if(selectedneighbor < boundary) {
                 if (activeNodes(selectedneighbor) != 0) {
    println("selectedneighbor "+ selectedneighbor + ":	Visit Count " + activeNodes(selectedneighbor))
			activeNodes(selectedneighbor)=activeNodes(selectedneighbor)+1
		 //   if(activeNodes(selectedneighbor) > visitgossip_max)  
	             workers(selectedneighbor) ! Gossiping()
		    //else
                    // workers(selectedneighbor) ! removenode(selectedneighbor)

                 
               }
             }
           // import system.dispatcher

	     //       val retry = scala.concurrent.duration.FiniteDuration(30, "milliseconds")
              //context.system.scheduler.scheduleOnce(retry, self, Gossiping())

           case "3Dimp" =>
               if (!sender.equals(self)) 
               noofvisit = noofvisit + 1
               selectedneighbor = ThreeD_findneighbor(currentnode, true, l,row,col,ht)
               val n = math.ceil(math.cbrt(numofnodes)).toInt
               val boundary = n * n * n
		
		//println("\n Boundary " + boundary + " selectedneighbor " + selectedneighbor)
               if(selectedneighbor < boundary) {
                 if (activeNodes(selectedneighbor) != 0) {
    println("selectedneighbor "+ selectedneighbor + ":	Visit Count " + activeNodes(selectedneighbor))
			activeNodes(selectedneighbor)=activeNodes(selectedneighbor)+1
                     workers(selectedneighbor) ! Gossiping()

                 
               }
             }
 	     //   import system.dispatcher
	       //     val retry = scala.concurrent.duration.FiniteDuration(30, "milliseconds")
	         //   context.system.scheduler.scheduleOnce(retry, self, Gossiping())

         }
		

       }

     case Pushsumpair(s: Double, w: Double) =>
             if (completed == false) {
               if (noofvisit == 0)
                 Lead ! nodevisited(currentnode, selectedneighbor)
               if (noofvisit == visitpushsum_max) {
                 completed = true
                 Lead ! removenode(currentnode)
               }
               topo match {
                 case "line" =>
                   if (!sender.equals(self)) 
                     var ratio_prev = s / w
                     s_self = s + s_self
                     w_self = w + w_self
                     var ratio_cur = s_self / w_self
                     if (math.abs(ratio_prev - ratio_cur) < Killratio)
                       noofvisit = noofvisit + 1
                     else
                       noofvisit = 0

                     selectedneighbor = line_findneighbor(currentnode)
                     s_self = s_self/2
                     w_self = w_self/2
                     workers(selectedneighbor) ! Pushsumpair(s_self , w_self )
                   
               case "3D"=>
                   if (!sender.equals(self)) 
                     var ratio_prev = s / w
                     s_self = s + s_self
                     w_self = w + w_self
                     var ratio_cur = s_self / w_self
                     if (math.abs(ratio_prev - ratio_cur) < Killratio)
                       noofvisit = noofvisit + 1
                     else
                       noofvisit = 0

               	 selectedneighbor = ThreeD_findneighbor(currentnode, false, l,row,col,ht)
                     s_self = s_self/2
                     w_self = w_self/2
                     workers(selectedneighbor) ! Pushsumpair(s_self , w_self )
                   
                  case "3dimp"=>
                   if (!sender.equals(self)) 
                     var ratio_prev = s / w
                     s_self = s + s_self
                     w_self = w + w_self
                     var ratio_cur = s_self / w_self
                     if (math.abs(ratio_prev - ratio_cur) < Killratio)
                       noofvisit = noofvisit + 1
                     else
                       noofvisit = 0

               	 selectedneighbor = ThreeD_findneighbor(currentnode, false, l,row,col,ht)
                     s_self = s_self/2
                     w_self = w_self/2
                     workers(selectedneighbor) ! Pushsumpair(s_self , w_self )
                   
           
               }
             }


           case _ =>
         }

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
             //return neighbours(selectnext)
           }

         }
         def ThreeD_findneighbor(currentnode : Int, check3dimp: Boolean, numneigh: Int, k: Int, j: Int, i: Int):Int ={
           var l = 0
           val n = math.ceil(math.cbrt(numofnodes)).toInt
           var neigbourarray = new Array[Int] (7)

           if(k>0){
             neigbourarray(l)=i+ n *(j+ n*(k-1)) // index as [i][j][k-1]
             l=l+1
           }
           if(k<numofnodes){
             neigbourarray(l)=i+ n *(j+ n*k+1)
             l=l+1
           }
           if(j>0){
             neigbourarray(l)=i+ n *(j-1+ n*k)
             l=l+1
           }
           if(j<numofnodes){
             neigbourarray(l)=i+ n *(j+1 + n*k)
             l=l+1
           }
           if(i>0){
             neigbourarray(l)=i-1 + n *(j+ n*k)
             l=l+1
           }
           if(i<numofnodes){
             neigbourarray(l)=i+1+ n *(j+ n*k)
            	 l=l+1
           }
	   if(check3dimp){
		  l=l+1
             neigbourarray(l)=Random.nextInt(n*n*n -1)
           

	}
        
            var selectnext = Random.nextInt(l)
  	// println("\n l "+l+"selectnext"+selectnext)
           return neigbourarray(selectnext)
         }
def pingself()  {
	val system =  akka.actor.ActorSystem("system")
	import system.dispatcher
 	
	val retry = scala.concurrent.duration.FiniteDurtion(10, "millisecond")
	context.system.scheduler.scheduleOnce(retry, self, Gossiping())
       }



