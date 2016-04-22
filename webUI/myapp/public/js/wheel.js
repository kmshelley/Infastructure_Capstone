// JavaScript source code	
var leftRect, yArray, heightArray, yearLabels,margin;

var NumElements = 7;
var centerIndex = 0;

var containerWidth = 200;
var containerHeight = 200;

	 

/*	 
var data = {};
window.onload = function() {data = loaddata(); updatedata(); }

function updatedata()
{
    var region = document.getElementById("region").value;
    var year = document.getElementById("year").value;
    var valuetype = document.getElementById("units").value;

    //based on the year selected set index
    setCenterIndex(year);
    var wheeldata = getwheeldata(region, year, valuetype, 2);
    drawwheel(wheeldata);
}*/

//////////////////////////////////////////////////////////////////////////////////////
//// HELP FUNCTIONS
//////////////////////////////////////////////////////////////////////////////////////		
//helper function - returns current y position
function Summation(m) {
    total = 0;
       
    for (i = m; i >= 0; i--) {
         
        if ( i === 0) {
            return total;
        } else {
            total +=ElementReductions[i-1]*hgt+barPadding*PaddingReductions[i];
	   
        }                  
    }
}
     
     
//Helper function to generate list of reductions to be applied to the width of graph based on number of elements
function GenerateElementList(m) {
    ElementList=[];
    half = (m-1)/2
    spread = .5/half
       
    for (i = 0; i < half ; i++) {    
        if ( i ===0) {
            ElementList.push(0.5);
        } else {
            ElementList.push(i*spread+0.5)
             
        }                  
    }
    ElementList.push(1.0)

    for (i = half-1; i > -1 ; i--) {    
        if ( i ===0) {
            ElementList.push(0.5);
        } else {
            ElementList.push(i*spread+0.5)    
        }                  
    }
    return ElementList;
}

//Helper function to generate list of reductions to be applied to the padding of graph based on number of elements
function GeneratePaddingList(m) {
    PaddingList=[0];
    half = (m-2)/2
    spread = 0.15/(half+1)
         
    for (i = 0; i <= half ; i++) {    
        if ( i ===half) {
            PaddingList.push(1.0);
        } else {
            PaddingList.push((i+1)*spread+0.5)
               
        }                  
    }
       
    for (i = half; i >= 0 ; i--) {    
        if ( i ===half) {
            PaddingList.push(1.0);
        } else {
            PaddingList.push((i+1)*spread+0.5)
             
        }                  
    }
    return PaddingList;
}
//////////////////////////////////////////////////////////////////////////////////////
//// HELPER FUNCTIONS END
//////////////////////////////////////////////////////////////////////////////////////	


//////////////////////////////////////////////////////////////////////////////////////
//// OBTAIN DATA AND PLACE IN ARRAY
//////////////////////////////////////////////////////////////////////////////////////	

function drawwheel(wheeljson)
{

    var dataset =[];        
    var years =[];
    for (bar in wheeljson){
        dataset.push(wheeljson[bar].electricity);
        years.push(wheeljson[bar].year);
    }
    displaywheel(dataset, years);
}
	 
     
function setCenterIndex(year) {
    centerIndex=year-minyear;
}

function getYearFromIndex(index) {
    return minyear + centerIndex;
}



function getTextY(i, downward) {

    //just enough Y value to move text below the bottom border
    yToHide = containerHeight * 2

    //coord to make text disappear from the page
    var disappearY = yToHide;

    if (typeof downward != "undefined") {
        (downward) ? yToHide : -50;
    }

    relativeCoord = getRelativeCoord(i);
    if (relativeCoord == -1) {
        return disappearY;
    } else {
        return (Summation(getRelativeCoord(i)) + hgt / 2 * ElementReductions[getRelativeCoord(i)] + barPadding * PaddingReductions[getRelativeCoord(i)] + margin.top) / ElementReductions[getRelativeCoord(i)];
    }
}

function getTextTransform(i) {
    scaleValue = .8333333;
    if (getRelativeCoord(i) != -1) {
        scaleValue = ElementReductions[getRelativeCoord(i)]
    }
    return "scale(1.0," + scaleValue + ")"

}

function getRelativeCoord(trueIndex, customCenter) {
    if (typeof customCenter == "undefined") {
        customCenter = centerIndex;

        var distanceFromCenter = trueIndex - centerIndex;

        if (Math.abs(distanceFromCenter) > ((NumElements - 1) / 2)) { //outside the range, don't display
            return -1;
        } else {
            return distanceFromCenter + (NumElements - 1) / 2;
        }
    }
}

var ElementReductions = [];
var hgt = 0;
var PaddingReductions = [];
     
function displaywheel(dataset, years)
{


       
    //This series of variables creates the array to be used to reduce elements later on.  Because the number of elements can change how the svg is filled up, the height will vary.  Algebra used to determine element total which will later feed the hgt variable to make sure the entire svg canvas is filled (except margins of course)
    ElementReductions = GenerateElementList(NumElements);
    var elementtotal = 0
    for ( var i = 0; i < ElementReductions.length; i++ ){
        elementtotal += ElementReductions[i];
    }
       
    //Similar to the above but figures out how large the padding between the bars should be.
    var barPaddingsize = 2
    var NumPaddings = NumElements - 1
    PaddingReductions = GeneratePaddingList(NumPaddings);
       
    var totalPadding = barPaddingsize*NumPaddings
       
    var paddingtotal = 0

    for ( var i = 0; i < PaddingReductions.length; i++ ){
        paddingtotal += PaddingReductions[i];
    }
       
    barPadding = totalPadding/paddingtotal
       
       
       
    //margins around the graphic - ensures that text is not cut off
       
    //THERE SHOULD BE A WAY TO APPLY TO MARGIN TO ALL SVG ITEMS IN A CANVAS RATHER THAN ONE AT A TIME THE WAY IT IS NOW	
       
    margin = {top: 15, right: 15, bottom: 15, left: 15},
        w = containerWidth - margin.left - margin.right,
        h = containerHeight - margin.top - margin.bottom;
       
    var scale = d3.scale.linear()
                  .domain([0, d3.max(dataset, function(d) { return d; })])
                  .range([0,w]);
    // Define the gradient
       
       
       
       
       
       
    hgt = (h - barPaddingsize*(ElementReductions.length-1))/elementtotal
       
    // Define the gradient
    yArray = new Array();
    heightArray = new Array();
			
    for (var q =0; q< ElementReductions.length; q++){
        heightArray.push(hgt * ElementReductions[q]);
        yArray.push(Summation(q)); 
    }
       
       
    // erase any pre-existing content
    d3.select("#wheel").selectAll("*").remove();
    //Create SVG element - Canvas
    var svg = d3.select("#wheel")
        .append("svg")
        .attr("width", w + margin.left + margin.right)
        .attr("height", h + margin.top + margin.bottom)
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
       
    //Left rectangle.  This is the horziontal bar that actually shows the metric			
    leftRect = svg.selectAll("rect")
            .data(dataset)
            .enter()
            .append("g")
            .append("rect")				   
            .attr("y", function (d, i) {
                return Summation(getRelativeCoord(i));
            })
            .attr("x", 0)
            .attr("height", function (d, i) {
                if (getRelativeCoord(i) == -1) {
                    return 0;
                } else {
                    return hgt * ElementReductions[getRelativeCoord(i)];
                }
            })
            .attr("width", function(d ) {    return scale(d);})			   
            .attr("fill","url(#graygradient)")					
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
       
       
       
       
    var drag = d3.behavior.drag()
         .origin(function (d) { return d; })
         .on("dragstart", dragstarted)
         .on("drag", dragged)
         .on("dragend", dragended);

    //Text Labels
       
    yearLabels = svg.selectAll("text")
   .data(years)
   .enter()			   
   .append("text")			   
   .text(function(d) {        return d;   })
   .attr("y", function (d, i)
   {
       return getTextY(i);
   })
   .attr("x", 4*margin.left)
   .attr("font-family", "sans-serif")
   .attr("font-size", "12px")
   .attr("text-anchor", "middle")
   .attr("transform", function (d, i)
   {
       return getTextTransform(i);
   });



    //empty rect that allows the entire group to be selected for mouseover events
    /*
      svg.selectAll("dataset.rect")
   .data(dataset)
   .enter()
   .append("g")
   .append("rect")				   
   .attr("y", function (d, i) {
       if (getRelativeCoord(i) == -1) {
           return 0;
       } else {
           return hgt * ElementReductions[getRelativeCoord(i)];
       }
   })
   .attr("x", 0)
   .attr("height", function(d,i) {return  hgt * ElementReductions[i];})
   .attr("width", w)				   
   .attr("fill","none")					
   .attr("pointer-events","all")
   .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
   .on("mouseover", function() {
     d3.select(this)//.selectAll("text")						
       .attr("fill", "url(#redgradient)");
   })
   .on("mouseout", function(d) {
     d3.select(this)//.selectAll("text")
       .attr("fill", "none");
   });;
    */
       
    /*
    var outline = svg.append("rect")			   
         .attr("y",0)
         .attr("x",0)
         .attr("width",700)
         .attr("height",700)
         .attr("stroke","black")
         .attr("stroke-width",5)
         .attr("fill","none");
    */
    //////////////////////////////////////////////////////////////////////////////////////
    //// GRADIENT AND MASK
    //////////////////////////////////////////////////////////////////////////////////////
    //gradient	

    var graygradient = svg.append("svg:defs")
              .append("svg:linearGradient")
              .attr("id", "graygradient")
              .attr("x1", "0%")
              .attr("y1", "0%")
              .attr("x2", "0%")
              .attr("y2", "100%")
              .attr("spreadMethod", "pad")
              .attr("gradientUnits","userSpaceOnUse")
    // Define the gradient colors
    graygradient.append("svg:stop")
                .attr("offset", "0%")
                .attr("stop-color", "rgb(50,50,50)")
                .attr("stop-opacity", 1);

    graygradient.append("svg:stop")
        .attr("offset", "50%")
        .attr("stop-color", "lightgrey")
        .attr("stop-opacity", 1);
       
    graygradient.append("svg:stop")
        .attr("offset", "100%")
        .attr("stop-color", "rgb(50,50,50)")
        .attr("stop-opacity", 1);
       
    var redgradient = svg.append("svg:defs")
             .append("svg:linearGradient")
             .attr("id", "redgradient")
             .attr("x1", "0%")
             .attr("y1", "0%")
             .attr("x2", "0%")
             .attr("y2", "100%")
             .attr("spreadMethod", "pad")
             .attr("gradientUnits","userSpaceOnUse")
    // Define the gradient colors
    redgradient.append("svg:stop")
               .attr("offset", "0%")
               .attr("stop-color", "rgb(50,50,50)")
               .attr("stop-opacity", 1);

    redgradient.append("svg:stop")
       .attr("offset", "50%")
       .attr("stop-color", "red")
       .attr("stop-opacity", 1);
       
    redgradient.append("svg:stop")
       .attr("offset", "100%")
       .attr("stop-color", "rgb(50,50,50)")
       .attr("stop-opacity", 1);


    //fake rectangle(2 of them) to give impression that wheels are disappearing at bottom and top

    svg.append("g")
   .append("rect")
   .attr("y", (heightArray[NumElements - 1] / 2) + yArray[NumElements - 1])
   .attr("x", -3)
   .attr("height", 75)
   .attr("width",w*2)
   .attr("fill", "white")
   .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
     

    svg.append("g")
.append("rect")
.attr("y", 0)
.attr("x", -3)
.attr("height", heightArray[NumElements-1]/2)
.attr("width", w * 2)
.attr("fill", "white")
.attr("transform", "translate(" + margin.left + "," + margin.top + ")");
}
//}

function turnToSelectedYear(year) {
    var currentYear = getYearFromIndex(centerIndex);

    if (currentYear == year) {
        return;
    } else if (currentYear > year) {
        while (getYearFromIndex(centerIndex) != year) {
            turnWheel(true);
        }
    } else if (currentYear < year) {
        while (getYearFromIndex(centerIndex) != year) {
            turnWheel(false);
        }
    }

}


function turnWheel(downward) {
       
    if (getYearFromIndex(centerIndex) == minyear || getYearFromIndex(centerIndex) == maxyear) {
        return;
    }
    if (downward) {
        centerIndex--;
    } else {
        centerIndex++;
    };

    document.getElementById("year").value = getYearFromIndex(centerIndex);
    updatedata();

    leftRect.each(function (d, i) {
           
        d3.select(this)
        .transition()
        .attr("y", function () {

            if (getRelativeCoord(i) == -1) {
                return (downward) ? containerHeight * 2 : -50;
            } else {
                return Summation(getRelativeCoord(i));

            }
             
        })
        .attr("height", function () {
            //next level reached 
     
            
                if (getRelativeCoord(i) == -1) {
                    return 0;
                } else {

                    return hgt * ElementReductions[getRelativeCoord(i)];
                }

            //;}
        });


        //.attr("height", function(d,i) {return  hgt * ElementReductions[i];})
    })
    /*} else {
        currentIncrement=0;
        //next level reached 
        if (downward)
        {
            centerIndex++;
            refresh();
        }
    }*/
    yearLabels.each(function (d, i) {
             
        d3.select(this)
        .transition()
        .attr("y", function () {

            return getTextY(i, downward);

        })
        .attr("transform", function () {
            return getTextTransform(i);
        });


        //.attr("height", function(d,i) {return  hgt * ElementReductions[i];})
    })

}


/*
function turnWheelByYValue(originalY, newY)
{
    var diff = newY - originalY;
    var goUp = true;

    //find out if direction is up or down
    if (diff== 0) return;
    goUp = (diff >0) ? true:  false; 

    if (goUp && getYearFromIndex(centerIndex) == maxyear) {
        return;
    }

    if (!goUp && getYearFromIndex(centerIndex) == minyear) {
        return;
    }


    if (downward) {
        centerIndex--;
    } else {
        centerIndex++;
    };
    leftRect.each(function (d, i) {

        d3.select(this)
        .transition()
        .attr("y", function () {

            if (getRelativeCoord(i) == -1) {
                return (downward) ? containerHeight * 2 : -50;
            } else {
                return Summation(getRelativeCoord(i));
            }

        })
        .attr("height", function () {
            //next level reached 

            if (downward) {
                if (getRelativeCoord(i) == -1) {
                    return 0;
                } else {

                    return hgt * ElementReductions[getRelativeCoord(i)];
                }
            }
            //;}
        });


        //.attr("height", function(d,i) {return  hgt * ElementReductions[i];})
    })

    yearLabels.each(function (d, i) {

        d3.select(this)
        .transition()
        .attr("y", function () {

            return getTextY(i, downward);

        })
        .attr("transform", function () {
            return getTextTransform(i);
        });


        //.attr("height", function(d,i) {return  hgt * ElementReductions[i];})
    })

}*/


function dragstarted(d) {
    d3.event.sourceEvent.stopPropagation();
    //d3.select(this).classed("dragging", true);
}

function dragged(d) {

    //d3.select(this).attr("cx", d.x = d3.event.x).attr("cy", d.y = d3.event.y);

}

function dragended(d) {
    d3.select(this).classed("dragging", false);
}
